# PostgreSQL Foreign Data Wrapper (FDW) Architecture

This document describes how PostgreSQL implements Foreign Data Wrappers, based on analysis
of the PostgreSQL source code and official documentation.

## 1. Overview

Foreign Data Wrappers (FDW) allow PostgreSQL to query external data sources (remote
databases, files, APIs, etc.) as if they were local tables. The FDW system was introduced
in SQL/MED (Management of External Data) and has been in PostgreSQL since version 9.1.

The architecture consists of four layers:

```
SQL Layer:     CREATE FOREIGN DATA WRAPPER / SERVER / TABLE / USER MAPPING
                        |
Catalog Layer: pg_foreign_data_wrapper / pg_foreign_server / pg_foreign_table / pg_user_mapping
                        |
Planner Layer: GetForeignRelSize -> GetForeignPaths -> GetForeignPlan
                        |
Executor Layer: BeginForeignScan -> IterateForeignScan -> EndForeignScan
```

## 2. Key Abstractions

### 2.1 System Catalogs

PostgreSQL stores FDW metadata in four system catalog tables:

**pg_foreign_data_wrapper** (`src/include/catalog/pg_foreign_data_wrapper.h`):
- `fdwname` - Name of the wrapper (e.g., `postgres_fdw`, `file_fdw`)
- `fdwhandler` - OID of the handler function that returns an `FdwRoutine` struct
- `fdwvalidator` - Optional function to validate options
- `fdwoptions` - Key-value options for the wrapper

**pg_foreign_server** (`src/include/catalog/pg_foreign_server.h`):
- `srvname` - Server name
- `srvfdw` - References which FDW handles this server
- `srvtype` / `srvversion` - Informational metadata
- `srvoptions` - Connection parameters (host, port, dbname, etc.)

**pg_foreign_table** (`src/include/catalog/pg_foreign_table.h`):
- `ftrelid` - References the table's `pg_class` entry
- `ftserver` - References which server owns this table
- `ftoptions` - Table-specific options (table name on remote, etc.)

**pg_user_mapping** (`src/include/catalog/pg_user_mapping.h`):
- `umuser` - Local PostgreSQL user (InvalidOid = PUBLIC)
- `umserver` - References the foreign server
- `umoptions` - Credentials (username, password, etc.)

### 2.2 Runtime Data Structures

**`foreign.h` (lines 24-58)** defines runtime structs that mirror the catalogs:

```c
typedef struct ForeignDataWrapper {
    Oid fdwid, owner;
    char *fdwname;
    Oid fdwhandler, fdwvalidator;
    List *options;           // key-value pairs as DefElem list
} ForeignDataWrapper;

typedef struct ForeignServer {
    Oid serverid, fdwid, owner;
    char *servername, *servertype, *serverversion;
    List *options;
} ForeignServer;

typedef struct UserMapping {
    Oid umid, userid, serverid;
    List *options;
} UserMapping;

typedef struct ForeignTable {
    Oid relid, serverid;
    List *options;
} ForeignTable;
```

## 3. The FdwRoutine Callback Interface

Every FDW must implement a **handler function** that returns a `FdwRoutine` struct
(`src/include/foreign/fdwapi.h`, lines 204-281). This struct is a vtable of function
pointers. Here are the categories:

### 3.1 Required Scan Callbacks

These are mandatory for every FDW:

```c
// Planning phase - estimate table size
void GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);

// Planning phase - generate access paths (scan strategies)
void GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);

// Planning phase - create ForeignScan plan node from chosen path
ForeignScan *GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
                            Oid foreigntableid, ForeignPath *best_path,
                            List *tlist, List *scan_clauses, Plan *outer_plan);

// Execution - initialize scan state, open connections
void BeginForeignScan(ForeignScanState *node, int eflags);

// Execution - fetch one row, fill TupleTableSlot
TupleTableSlot *IterateForeignScan(ForeignScanState *node);

// Execution - restart scan from beginning (for nested loops)
void ReScanForeignScan(ForeignScanState *node);

// Execution - cleanup, close connections
void EndForeignScan(ForeignScanState *node);
```

### 3.2 DML Callbacks (INSERT/UPDATE/DELETE)

Optional, for writable foreign tables:

```c
void AddForeignUpdateTargets(...);      // Add hidden columns for row identity
List *PlanForeignModify(...);           // Plan modification operations
void BeginForeignModify(...);           // Initialize modification state
TupleTableSlot *ExecForeignInsert(...); // Insert one row
TupleTableSlot *ExecForeignUpdate(...); // Update one row
TupleTableSlot *ExecForeignDelete(...); // Delete one row
void EndForeignModify(...);             // Cleanup after modifications
int IsForeignRelUpdatable(Relation rel); // Report supported operations
```

### 3.3 Pushdown Callbacks

For performance optimization:

```c
// Push joins to remote server
void GetForeignJoinPaths(PlannerInfo *root, RelOptInfo *joinrel,
                         RelOptInfo *outerrel, RelOptInfo *innerrel,
                         JoinType jointype, JoinPathExtraData *extra);

// Push aggregation/sorting/grouping to remote server
void GetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
                          RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra);

// Execute UPDATE/DELETE directly on remote (without local processing)
bool PlanDirectModify(...);
void BeginDirectModify(...);
TupleTableSlot *IterateDirectModify(...);
void EndDirectModify(...);
```

### 3.4 Support Callbacks

```c
void ExplainForeignScan(...);           // Custom EXPLAIN output
bool AnalyzeForeignTable(...);          // Collect statistics
List *ImportForeignSchema(...);         // Auto-create foreign tables from remote schema
void ExecForeignTruncate(...);          // TRUNCATE support
```

### 3.5 Parallel and Async Callbacks

```c
bool IsForeignScanParallelSafe(...);
void ForeignAsyncRequest(AsyncRequest *areq);
void ForeignAsyncConfigureWait(AsyncRequest *areq);
void ForeignAsyncNotify(AsyncRequest *areq);
```

## 4. SQL Syntax

### 4.1 Creating an FDW

```sql
-- Register the wrapper
CREATE FOREIGN DATA WRAPPER my_fdw
    HANDLER my_fdw_handler
    VALIDATOR my_fdw_validator;

-- Define a server
CREATE SERVER remote_server
    FOREIGN DATA WRAPPER my_fdw
    OPTIONS (host 'remote.example.com', port '5432', dbname 'mydb');

-- Map local user to remote credentials
CREATE USER MAPPING FOR current_user
    SERVER remote_server
    OPTIONS (user 'remote_user', password 'secret');

-- Create a foreign table
CREATE FOREIGN TABLE remote_users (
    id      integer,
    name    text,
    email   text
) SERVER remote_server
OPTIONS (schema_name 'public', table_name 'users');
```

### 4.2 Querying

Foreign tables behave like local tables:

```sql
SELECT * FROM remote_users WHERE id > 100;
SELECT r.name, l.department
  FROM remote_users r JOIN local_departments l ON r.dept_id = l.id;
INSERT INTO remote_users (name, email) VALUES ('Alice', 'alice@example.com');
```

### 4.3 Schema Import

```sql
IMPORT FOREIGN SCHEMA public FROM SERVER remote_server INTO local_schema;
```

## 5. Query Planner Integration

The planner interacts with FDW through a well-defined flow
(`src/backend/optimizer/path/allpaths.c`):

### Phase 1: Sizing

```
set_foreign_size() calls:
  -> set_foreign_size_estimates()    -- initial row/width estimates
  -> fdwroutine->GetForeignRelSize() -- FDW adjusts estimates
  -> clamp_row_est()                 -- sanity check
```

The FDW populates `baserel->rows`, `baserel->tuples`, and `baserel->width`.

### Phase 2: Path Generation

```
set_foreign_pathlist() calls:
  -> fdwroutine->GetForeignPaths()   -- FDW creates ForeignPath nodes via add_path()
```

Each `ForeignPath` includes cost estimates and can represent different strategies
(full scan vs. filtered scan, different index strategies, etc.).

### Phase 3: Plan Creation

```
create_foreignscan_plan() calls:
  -> fdwroutine->GetForeignPlan()    -- FDW creates ForeignScan plan node
```

The FDW embeds execution data in `fdw_private` (opaque list) and expressions
to evaluate at runtime in `fdw_exprs`.

### Predicate Pushdown

The planner provides `baserel->baserestrictinfo` containing WHERE clause conditions.
The FDW can:
1. Identify which conditions it can evaluate remotely
2. Remove them from `scan_clauses` in `GetForeignPlan`
3. Send them to the remote source during execution
4. Add them to `fdw_recheck_quals` for correctness verification

## 6. Executor Integration

**`src/backend/executor/nodeForeignscan.c`** implements the execution flow:

```
ExecInitForeignScan()
  -> GetFdwRoutineForRelation()    -- look up handler, get FdwRoutine
  -> fdwroutine->BeginForeignScan() -- FDW allocates fdw_state

ExecForeignScan() [main loop]
  -> ForeignNext()
     -> fdwroutine->IterateForeignScan()
        -- FDW fills slot->tts_values[] and slot->tts_isnull[]
        -- calls ExecStoreVirtualTuple(slot)
        -- returns empty slot when done

ExecEndForeignScan()
  -> fdwroutine->EndForeignScan()  -- cleanup
```

### TupleTableSlot

The data exchange structure between FDW and executor:

```c
typedef struct TupleTableSlot {
    TupleDesc   tts_tupleDescriptor;  // Column definitions
    Datum      *tts_values;           // Array of column values
    bool       *tts_isnull;           // Array of NULL flags
    // ... other fields
} TupleTableSlot;
```

FDW fills `tts_values[i]` and `tts_isnull[i]` for each column, then calls
`ExecStoreVirtualTuple(slot)` to mark it valid.

## 7. Concrete FDW Implementations

### 7.1 file_fdw (CSV/Text Files)

Location: `contrib/file_fdw/`

The simplest FDW. Read-only, no pushdown:

```c
Datum file_fdw_handler(PG_FUNCTION_ARGS) {
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);
    fdwroutine->GetForeignRelSize = fileGetForeignRelSize;   // stat() file for size
    fdwroutine->GetForeignPaths = fileGetForeignPaths;       // single sequential path
    fdwroutine->GetForeignPlan = fileGetForeignPlan;         // no pushdown
    fdwroutine->BeginForeignScan = fileBeginForeignScan;     // open file, create CopyState
    fdwroutine->IterateForeignScan = fileIterateForeignScan; // NextCopyFrom() per row
    fdwroutine->EndForeignScan = fileEndForeignScan;         // close file
    PG_RETURN_POINTER(fdwroutine);
}
```

Execution is straightforward: open file, read rows with COPY parser, return tuples.

### 7.2 postgres_fdw (Remote PostgreSQL)

Location: `contrib/postgres_fdw/`

Full-featured FDW with pushdown:

- **Planning**: Classifies WHERE clauses as remote vs. local, builds remote SQL query
- **Execution**: Opens libpq connection, sends query, fetches results in batches
- **DML**: Translates INSERT/UPDATE/DELETE to remote SQL
- **Pushdown**: Supports join pushdown, aggregate pushdown, ORDER BY pushdown
- **Connection caching**: Reuses connections across queries

Key state structures:
- `PgFdwRelationInfo` - Planning state (remote/local conditions, cost estimates)
- `PgFdwScanState` - Execution state (connection, query text, result cache)
- `PgFdwModifyState` - DML state (prepared statements, target columns)

### 7.3 duckdb_fdw (DuckDB Integration)

External project ([alitrack/duckdb_fdw](https://github.com/alitrack/duckdb_fdw)):

```sql
CREATE EXTENSION duckdb_fdw;
CREATE SERVER duckdb_server FOREIGN DATA WRAPPER duckdb_fdw
    OPTIONS (database '/path/to/analytics.duckdb');
CREATE FOREIGN TABLE sales (id int, amount float, date date)
    SERVER duckdb_server OPTIONS (table 'sales');

-- Pushdown: WHERE, GROUP BY, ORDER BY, JOIN, LIMIT, aggregates
SELECT date, SUM(amount) FROM sales GROUP BY date ORDER BY date;
```

Supports: predicate pushdown, aggregate pushdown, IMPORT FOREIGN SCHEMA,
batch INSERT, TRUNCATE, connection caching.

### 7.4 HTTP/REST FDWs (via Multicorn)

[Multicorn](https://multicorn.org/) provides a Python bridge for writing FDWs:

```python
class RestApiWrapper(ForeignDataWrapper):
    def __init__(self, options, columns):
        self.url = options['url']
        self.columns = columns

    def execute(self, quals, columns):
        response = requests.get(self.url)
        for row in response.json():
            yield {col: row.get(col) for col in columns}
```

```sql
CREATE SERVER rest_server FOREIGN DATA WRAPPER multicorn
    OPTIONS (wrapper 'myfdw.RestApiWrapper');
CREATE FOREIGN TABLE api_users (id int, name text, email text)
    SERVER rest_server OPTIONS (url 'https://api.example.com/users');
```

## 8. Design Principles

1. **Layered abstraction**: FDW -> Server -> Table -> User Mapping provides clean separation
2. **Cost-based optimization**: FDWs participate in query planning through cost estimates
3. **Predicate pushdown**: Reduces data transfer by filtering at the source
4. **Transparency**: Foreign tables look and behave like local tables in SQL
5. **Extensibility**: Any data source can be wrapped with a handler function
6. **Lazy evaluation**: IterateForeignScan returns one row at a time (iterator pattern)
7. **Resource management**: Clear Begin/End lifecycle for connections and state

## 9. Summary Comparison Table

| Aspect | file_fdw | postgres_fdw | duckdb_fdw |
|--------|----------|--------------|------------|
| Read | Yes | Yes | Yes |
| Write | No | Yes | Yes |
| Predicate pushdown | No | Yes | Yes |
| Join pushdown | No | Yes | No |
| Aggregate pushdown | No | Yes | Yes |
| Parallel scan | Yes | No | No |
| Async execution | No | Yes | No |
| Connection caching | N/A | Yes | Yes |

## References

- [PostgreSQL FDW Callbacks Documentation](https://www.postgresql.org/docs/current/fdw-callbacks.html)
- [PostgreSQL FDW Query Planning](https://www.postgresql.org/docs/current/fdw-planning.html)
- [PostgreSQL FDW Wiki](https://wiki.postgresql.org/wiki/Foreign_data_wrappers)
- [duckdb_fdw GitHub](https://github.com/alitrack/duckdb_fdw)
- [Multicorn FDW Library](https://multicorn.org/)
