package sqlancer.limbo;

import java.io.File;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Options;
import sqlancer.sqlite3.SQLite3SpecialStringGenerator;
import sqlancer.sqlite3.gen.SQLite3ExplainGenerator;
import sqlancer.sqlite3.gen.SQLite3TransactionGenerator;
import sqlancer.sqlite3.gen.ddl.SQLite3DropIndexGenerator;
import sqlancer.sqlite3.gen.ddl.SQLite3DropTableGenerator;
import sqlancer.sqlite3.gen.ddl.SQLite3IndexGenerator;
import sqlancer.sqlite3.gen.ddl.SQLite3TableGenerator;
import sqlancer.sqlite3.gen.ddl.SQLite3ViewGenerator;
import sqlancer.sqlite3.gen.dml.SQLite3DeleteGenerator;
import sqlancer.sqlite3.gen.dml.SQLite3InsertGenerator;
import sqlancer.sqlite3.gen.dml.SQLite3UpdateGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

/**
 * SQLancer provider for Limbo (Turso's SQLite-compatible database).
 *
 * This provider is based on SQLite3Provider but adapted for Limbo's
 * current compatibility level. Features not yet supported by Limbo
 * are disabled or have their errors filtered appropriately.
 */
@AutoService(DatabaseProvider.class)
public class LimboProvider extends SQLProviderAdapter<SQLite3GlobalState, SQLite3Options> {

    public static boolean allowFloatingPointFp = true;
    public static boolean mustKnowResult;

    // Limbo-compatible pragmas only (cache_size is supported)
    private static final List<String> DEFAULT_PRAGMAS = Arrays.asList(
            "PRAGMA cache_size = 50000;"
    );

    // Errors that Limbo may produce for unsupported features
    // Note: These are substring matches, so partial matches work
    private static final List<String> LIMBO_EXPECTED_ERRORS = Arrays.asList(
            "Not a valid pragma name",
            "not yet implemented",
            "not yet supported",
            "not supported",
            "not implemented",
            "unsupported",
            "Parse error",
            "TEMPORARY table",
            "ON CONFLICT",
            "INSERT OR",
            "UPDATE OR",
            "WITHOUT ROWID",
            "COLLATE",
            "AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY",
            "no such table",
            "cannot rollback - no transaction is active",
            "cannot commit - no transaction is active",
            "cannot start a transaction within a transaction",
            "INDEXED BY",
            "NOT INDEXED",
            "UNIQUE constraint failed"
    );

    public LimboProvider() {
        super(SQLite3GlobalState.class, SQLite3Options.class);
    }

    public enum Action implements AbstractAction<SQLite3GlobalState> {
        // Supported actions (subset of SQLite3)
        CREATE_INDEX(SQLite3IndexGenerator::insertIndex),
        CREATE_VIEW(SQLite3ViewGenerator::generate),
        CREATE_TABLE(SQLite3TableGenerator::createRandomTableStatement),
        INSERT(SQLite3InsertGenerator::insertRow),
        DELETE(SQLite3DeleteGenerator::deleteContent),
        UPDATE(SQLite3UpdateGenerator::updateRow),
        DROP_INDEX(SQLite3DropIndexGenerator::dropIndex),
        DROP_TABLE(SQLite3DropTableGenerator::dropTable),
        DROP_VIEW(SQLite3ViewGenerator::dropView),
        EXPLAIN(SQLite3ExplainGenerator::explain),
        TRANSACTION_START(SQLite3TransactionGenerator::generateBeginTransaction) {
            @Override
            public boolean canBeRetried() {
                return false;
            }
        },
        ROLLBACK_TRANSACTION(SQLite3TransactionGenerator::generateRollbackTransaction) {
            @Override
            public boolean canBeRetried() {
                return false;
            }
        },
        COMMIT(SQLite3TransactionGenerator::generateCommit) {
            @Override
            public boolean canBeRetried() {
                return false;
            }
        };

        // Disabled actions (not supported by Limbo yet):
        // - PRAGMA (most pragmas not supported)
        // - CREATE_TRIGGER
        // - CREATE_VIRTUALTABLE (FTS)
        // - CREATE_RTREETABLE
        // - VACUUM
        // - REINDEX
        // - ANALYZE
        // - ALTER (limited support)

        private final SQLQueryProvider<SQLite3GlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<SQLite3GlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(SQLite3GlobalState state) throws Exception {
            SQLQueryAdapter baseQuery = sqlQueryProvider.getQuery(state);
            // Wrap all queries with Limbo's expected errors
            ExpectedErrors errors = new ExpectedErrors();
            errors.addAll(LIMBO_EXPECTED_ERRORS);
            // Also add standard SQLite3 errors
            SQLite3Errors.addExpectedExpressionErrors(errors);
            SQLite3Errors.addInsertNowErrors(errors);
            SQLite3Errors.addMatchQueryErrors(errors);
            SQLite3Errors.addDeleteErrors(errors);
            return new SQLQueryAdapter(baseQuery.getQueryString(), errors, baseQuery.couldAffectSchema());
        }
    }

    private static int mapActions(SQLite3GlobalState globalState, Action a) {
        int nrPerformed = 0;
        Randomly r = globalState.getRandomly();
        switch (a) {
        case CREATE_VIEW:
            nrPerformed = r.getInteger(0, 2);
            break;
        case DELETE:
        case DROP_VIEW:
        case DROP_INDEX:
            nrPerformed = r.getInteger(0, 0);
            break;
        case EXPLAIN:
        case DROP_TABLE:
            nrPerformed = r.getInteger(0, 0);
            break;
        case INSERT:
            nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            break;
        case CREATE_INDEX:
            nrPerformed = r.getInteger(0, 5);
            break;
        case UPDATE:
            nrPerformed = r.getInteger(0, 30);
            break;
        case CREATE_TABLE:
            nrPerformed = 0;
            break;
        case TRANSACTION_START:
        case ROLLBACK_TRANSACTION:
        case COMMIT:
        default:
            nrPerformed = r.getInteger(1, 10);
            break;
        }
        return nrPerformed;
    }

    @Override
    public void generateDatabase(SQLite3GlobalState globalState) throws Exception {
        Randomly r = new Randomly(SQLite3SpecialStringGenerator::generate);
        globalState.setRandomly(r);
        if (globalState.getDbmsSpecificOptions().generateDatabase) {

            addSensiblePragmaDefaults(globalState);
            int nrTablesToCreate = 1;
            if (Randomly.getBoolean()) {
                nrTablesToCreate++;
            }
            while (Randomly.getBooleanWithSmallProbability()) {
                nrTablesToCreate++;
            }
            int i = 0;

            do {
                SQLQueryAdapter tableQuery = getTableQuery(globalState, i++);
                globalState.executeStatement(tableQuery);
            } while (globalState.getSchema().getDatabaseTables().size() < nrTablesToCreate);

            checkTablesForGeneratedColumnLoops(globalState);

            StatementExecutor<SQLite3GlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                    LimboProvider::mapActions, (q) -> {
                        if (q.couldAffectSchema() && globalState.getSchema().getDatabaseTables().isEmpty()) {
                            throw new IgnoreMeException();
                        }
                    });
            se.executeStatements();

            SQLQueryAdapter query = SQLite3TransactionGenerator.generateCommit(globalState);
            globalState.executeStatement(query);

            query = SQLite3TransactionGenerator.generateRollbackTransaction(globalState);
            globalState.executeStatement(query);
        }
    }

    private void checkTablesForGeneratedColumnLoops(SQLite3GlobalState globalState) throws Exception {
        ExpectedErrors errors = new ExpectedErrors();
        errors.addAll(LIMBO_EXPECTED_ERRORS);
        errors.add("generated column loop");
        errors.add("integer overflow");

        for (SQLite3Table table : globalState.getSchema().getDatabaseTables()) {
            SQLQueryAdapter q = new SQLQueryAdapter("SELECT * FROM " + table.getName(), errors);
            if (!q.execute(globalState)) {
                throw new IgnoreMeException();
            }
        }
    }

    private SQLQueryAdapter getTableQuery(SQLite3GlobalState globalState, int i) {
        // Only create normal tables (no virtual tables for Limbo)
        String tableName = DBMSCommon.createTableName(i);
        SQLQueryAdapter baseQuery = SQLite3TableGenerator.createTableStatement(tableName, globalState);
        // Wrap with Limbo's expected errors
        ExpectedErrors errors = ExpectedErrors.from(
                LIMBO_EXPECTED_ERRORS.toArray(new String[0]));
        return new SQLQueryAdapter(baseQuery.getQueryString(), errors, baseQuery.couldAffectSchema());
    }

    private void addSensiblePragmaDefaults(SQLite3GlobalState globalState) throws Exception {
        ExpectedErrors errors = new ExpectedErrors();
        errors.addAll(LIMBO_EXPECTED_ERRORS);

        for (String s : DEFAULT_PRAGMAS) {
            SQLQueryAdapter q = new SQLQueryAdapter(s, errors);
            globalState.executeStatement(q);
        }
    }

    @Override
    public SQLConnection createDatabase(SQLite3GlobalState globalState) throws SQLException {
        // Explicitly load the Turso JDBC driver
        try {
            Class.forName("tech.turso.JDBC");
        } catch (ClassNotFoundException e) {
            throw new SQLException("Could not load Turso JDBC driver: " + e.getMessage(), e);
        }

        File dir = new File("." + File.separator + "databases");
        if (!dir.exists()) {
            dir.mkdir();
        }
        File dataBase = new File(dir, globalState.getDatabaseName() + ".db");
        if (dataBase.exists() && globalState.getDbmsSpecificOptions().deleteIfExists) {
            dataBase.delete();
        }
        // Use Limbo JDBC driver
        String url = "jdbc:turso:" + dataBase.getAbsolutePath();
        return new SQLConnection(DriverManager.getConnection(url));
    }

    @Override
    public String getDBMSName() {
        return "limbo";
    }

    @Override
    public String getQueryPlan(String selectStr, SQLite3GlobalState globalState) throws Exception {
        String queryPlan = "";
        ExpectedErrors errors = new ExpectedErrors();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addMatchQueryErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
        errors.addAll(LIMBO_EXPECTED_ERRORS);

        SQLQueryAdapter q = new SQLQueryAdapter(SQLite3ExplainGenerator.explain(selectStr), errors);
        try (SQLancerResultSet rs = q.executeAndGet(globalState)) {
            if (rs != null) {
                while (rs.next()) {
                    queryPlan += rs.getString(4) + ";";
                }
            }
        } catch (SQLException | AssertionError e) {
            queryPlan = "";
        }
        return queryPlan;
    }

    @Override
    protected double[] initializeWeightedAverageReward() {
        return new double[Action.values().length];
    }

    @Override
    protected void executeMutator(int index, SQLite3GlobalState globalState) throws Exception {
        SQLQueryAdapter queryMutateTable = Action.values()[index].getQuery(globalState);
        globalState.executeStatement(queryMutateTable);
    }

    @Override
    protected boolean addRowsToAllTables(SQLite3GlobalState globalState) throws Exception {
        List<SQLite3Table> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (SQLite3Table table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = SQLite3InsertGenerator.insertRow(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }
}
