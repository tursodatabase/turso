/**********************************************************************\
**  source       * tirdb.c
**  description  * TIRDB handling functions.
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#include "tirdb.h"
#include "tatpversion.h"
#include "remcontrol.h"


/*##**********************************************************************\
 *
 *      addToTIRDB
 *
 * Add a missing value to the TIRDB
 *
 * Parameters :
 *      tirdb
 *          Connect handle of TIRDB
 *      table_name
 *          Name of TIRDB table to be checked.
 *      ddf 
 *          The data definition file structure       
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int addToTIRDB(SQLHDBC tirdb, char *table_name, struct ddfs *ddf)
{
        RETCODE err;
        SQLHSTMT statement;
        char cmd[512]; 
            
        if (strcmp(table_name, "hardware") == 0) {
            sprintf(cmd, "INSERT INTO %s (hardware_id) VALUES ('%s')",
                    table_name, ddf->hardware_id);
        } else if (strcmp(table_name, "operating_systems") == 0) {
            sprintf(cmd, "INSERT INTO %s (name, version) VALUES ('%s','%s')",
                    table_name, ddf->os_name, ddf->os_version);
        } else if (strcmp(table_name, "_databases") == 0) {
            sprintf(cmd, "INSERT INTO %s (name, version) VALUES ('%s','%s')",
                    table_name, ddf->db_name, ddf->db_version);
        } else {
            return E_ERROR;
        }                   
        writeLog('D', cmd);
               
        err = SQLAllocHandle(SQL_HANDLE_STMT, tirdb, &statement);
        if (error_c(tirdb, err)) {
            writeLog('E', "SQLAllocHandle failed");
            return E_ERROR;
        }        
        err = SQLPrepare(statement,
                         CHAR2SQL(cmd),
                         SQL_NTS);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLPrepare failed");
            return E_ERROR;
        }
        err = SQLExecute(statement);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLExecute failed");
            return E_ERROR;
        }
        err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLFreeHandle failed");
        }

        return E_OK;
        
}

/*##**********************************************************************\
 *
 *      checkOneFromTIRDB
 *
 * Performs a row check from TIRDB. If the query (the table and the where
 * clause given as parameters) return one row the method returns
 * successfully. Otherwise an error is returned. Used by checkTIRDB().
 *
 * Parameters :
 *      tirdb
 *          Connect handle of TIRDB
 *      table_name
 *          Name of TIRDB table to be checked.
 *      where_clause
 *          SQL where clause applied in the check.
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int checkOneFromTIRDB(SQLHDBC tirdb, char *table_name, char *where_clause)
{
        RETCODE err;
        SQLHSTMT statement;
        int count = 0;
        char msg[256];
        char cmd[256];
        
        /* The SQL clause to be used in the query */
        sprintf(cmd, "SELECT COUNT(*) FROM %s WHERE %s", table_name,
                where_clause);
        writeLog('D', cmd);
        
        err = SQLAllocHandle(SQL_HANDLE_STMT, tirdb, &statement);
        if (error_c(tirdb, err)) {
            writeLog('E', "SQLAllocHandle failed");
            return E_ERROR;
        }
        
        err = SQLPrepare(statement,
                         CHAR2SQL(cmd),
                         SQL_NTS);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLPrepare failed");
            return E_ERROR;
        }
        
        /* Bind the local variable 'count' */
        err = SQLBindCol(statement,
                         1,
                         SQL_C_SLONG,
                         &count,
                         0,
                         NULL);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindCol failed");
            return E_ERROR;
        }
        
        /* Execute the command */
        err = SQLExecute(statement);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLExecute failed");
            return E_ERROR;
        }
        err = SQLFetch(statement);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLFetch failed");
            return E_ERROR;
        }
        err = SQLCloseCursor(statement);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLCloseCursor failed");
        }
        
        /* Clean up */
        err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLFreeHandle failed");
        }
        
        /* Check the result of the query. If we got one row
           as the result then things are fine (the we were serching
           for exists). */
        if (count != 1) {
            if (count == 0) {
                sprintf(msg, "TIRDB table '%s' has no rows where %s",
                        table_name, where_clause);
            }
            else {
                sprintf(msg,
                        "TIRDB table '%s' has too many rows where %s",
                        table_name, where_clause);
            }
            writeLog('E', msg);
            return E_ERROR;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      checkTIRDB
 *
 * Verifies the TDF against TIRDB. The existence of the following data
 * in TIRDB is cerified:
 *  - hardware identifier
 *  - operating system name and version
 *  - target database name and version
 * All the information above is given in the DDF file.
 *
 * Parameters:
 *      ConnectString 
 *          Connect string to TIRDB
 *
 *      ddf
 *          The data definition file structure
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int checkTIRDB(char *ConnectString, struct ddfs *ddf, int add_missing)
{
        SQLHENV tirdb_env;
        SQLHDBC tirdb;
        char where_clause[256];
        int errors;

        writeLog('D', "Verifying TDF against TIRDB");
        /* Connect to TIRDB */
        if (ConnectDB(&tirdb_env, &tirdb, ConnectString, "TIRDB")) {
            return E_FATAL;
        }
        errors = 0;

        sprintf(where_clause, "hardware_id = '%s'", ddf->hardware_id);
        if (checkOneFromTIRDB(tirdb, "hardware", where_clause)) {
            /* 'errors' Indicates that the check did not pass */
            if (! add_missing) {
                errors++;
            } else {
                /* add missing value */
                if (addToTIRDB(tirdb, "hardware", ddf)) {
                    errors++;
                } else {
                    writeLog('I', "Option '-a' detected, added missing value "
                             "to 'hardware' table in TIRDB");
                }
            }
        }

        sprintf(where_clause, "name = '%s' AND version = '%s'",
                ddf->os_name, ddf->os_version);
        if (checkOneFromTIRDB(tirdb, "operating_systems", where_clause)) {
            /* 'errors' Indicates that the check did not pass */
            if (! add_missing) {
                errors++;
            } else {
                if (addToTIRDB(tirdb, "operating_systems", ddf)) {
                    errors++;
                } else {
                    writeLog('I', "Option '-a' detected, added missing values "
                             "to 'operating_systems' table in TIRDB");
                }
            }
        }

        sprintf(where_clause, "name = '%s' AND version = '%s'",
                ddf->db_name, ddf->db_version);
        if (checkOneFromTIRDB(tirdb, "_databases", where_clause)) {
            /* 'errors' Indicates that the check did not pass */
            if (! add_missing) {
                errors++;
            } else {
                if (addToTIRDB(tirdb, "_databases", ddf)) {
                    errors++;
                } else {
                    writeLog('I', "Option '-a' detected, added missing values "
                             "to '_databases' table in TIRDB");
                }
            }
        }

        DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
        if (errors > 0) {
            writeLog('I', "Edit the TDF or TIRDB");
            return E_ERROR;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      initialize_tirdb_for_session
 *
 * Initialize TIRDB for one session (DDF/TDF file). First figures out the
 * next available session id from TIRDB. Note, that we don't use automatic
 * column values in TIRDB. Then inserts a session row to TEST_SESSIONS.
 *
 * Parameters:
 *      ConnectString
 *          Connect string to TIRDB
 *
 *      ddf 
 *			Pointer to the data definition structure
 *
 *      tdf 
 *          Pointer to the test definition structure
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int initialize_tirdb_for_session(char *ConnectString, struct ddfs *ddf,
                                 struct tdfs *tdf)
{
        SQLHENV tirdb_env;
        SQLHDBC tirdb;
        SQLHSTMT statement;
        RETCODE err;
        SQLLEN ind = 0;
        SQLLEN nullData = SQL_NULL_DATA;
        SQLUSMALLINT ParameterNumber;
        /* The TATP software version */
        char prg_version[80];

        writeLog('D', "Storing session data to TIRDB");
        /* First connect to TIRDB */
        if (ConnectDB(&tirdb_env, &tirdb, ConnectString, "TIRDB")) {
            return E_ERROR;
        }

        /* This is a good place to save the target database
           configuration file if needed */
        saveConfigurationFile(tirdb, ddf);

        /* Now going to store data to table test_sessions.
           First getting the new session_id */
        err = SQLAllocHandle(SQL_HANDLE_STMT, tirdb, &statement);
        if (error_c(tirdb, err)) {
            writeLog('E', "SQLAllocHandle failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        /* Get the maximum value of the exiting session identifiers
           from TIRDB and then increase that value by one to get id
           for current session. If there is no session id yet in TIRDB
           then currentID = 1 */
        err = SQLPrepare(statement,
                         CHAR2SQL("SELECT MAX(session_id) FROM test_sessions"),
                         SQL_NTS);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLPrepare failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindCol(statement,
                         1,
                         SQL_C_SLONG,
                         &(tdf->session_id),
                         0,
                         &ind);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindCol failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLExecute(statement);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLExecute failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLFetch(statement);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLFetch failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLCloseCursor(statement);
        if (error_s(statement, err, NULL)) {
            writeLog('W', "SQLCloseCursor failed");
        }

        if (ind == SQL_NULL_DATA) {
            /* This is empty table, start with this key */
            tdf->session_id = 1;
        }
        else {
            /* New key is one more than max. key fetched */
            (tdf->session_id)++;
        }

        /* Now the session_id is determined.
           Store the DDF/TDF data to TIRDB */
        strncpy(prg_version, TATPVERSION, 80);

        /* Prepare the statement to insert the session row
           to the TEST_SESSIONS table */
        err = SQLPrepare(statement,
                         CHAR2SQL(TEST_SESSION_INSERT),
                         SQL_NTS);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLPrepare failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        /* Bind the session data from both the DDF and TDF
           structures */
        ParameterNumber = 1;
        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_SLONG,
                               SQL_INTEGER,
                               0,
                               0,
                               &(tdf->session_id),
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter session_id failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_CHAR,
                               SQL_CHAR,
                               TIRDB_SESSION_NAME_LEN,
                               0,
                               tdf->session_name,
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter session_name failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_DATE,
                               SQL_DATE,
                               0,
                               0,
                               &(tdf->start_date),
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter start_date failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_TIME,
                               SQL_TIME,
                               0,
                               0,
                               &(tdf->start_time),
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter start_time failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_DATE,
                               SQL_DATE,
                               0,
                               0,
                               &(tdf->stop_date),
                               0,
                               &nullData);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter stop_date failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_TIME,
                               SQL_TIME,
                               0,
                               0,
                               &(tdf->stop_time),
                               0,
                               &nullData);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter stop_time failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_CHAR,
                               SQL_CHAR,
                               TIRDB_SESSION_AUTHOR_LEN,
                               0,
                               tdf->author,
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter author failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_CHAR,
                               SQL_CHAR,
                               TIRDB_DB_NAME_LEN,
                               0,
                               ddf->db_name,
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter db_name failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_CHAR,
                               SQL_CHAR,
                               TIRDB_DB_VERSION_LEN,
                               0,
                               ddf->db_version,
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter db_version failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_CHAR,
                               SQL_CHAR,
                               TIRDB_DB_VERSION_LEN,
                               0,
                               ddf->hardware_id,
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter hardware_id failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_CHAR,
                               SQL_CHAR,
                               TIRDB_OS_NAME_LEN,
                               0,
                               ddf->os_name,
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter os_name failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_CHAR,
                               SQL_CHAR,
                               TIRDB_OS_VERSION_LEN,
                               0,
                               ddf->os_version,
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter os_version failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_SLONG,
                               SQL_INTEGER,
                               0,
                               0,
                               &(tdf->throughput_resolution),
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter throughput_resolution failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_CHAR,
                               SQL_CHAR,
                               TIRDB_CONFIG_ID_LEN,
                               0,
                               ddf->configuration_content_checksum,
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter config_id failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_CHAR,
                               SQL_CHAR,
                               TIRDB_CONFIG_NAME_LEN,
                               0,
                               ddf->configuration_code,
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter config_name failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_CHAR,
                               SQL_CHAR,
                               TIRDB_SOFTWARE_VERSION_LEN,
                               0,
                               prg_version,
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter software_version failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_CHAR,
                               SQL_CHAR,
                               TIRDB_SESSION_COMMENTS_LEN,
                               0,
                               tdf->comments,
                               0,
                               NULL);

        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter comments failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        /* ... and execute the statement */
        err = SQLExecute(statement);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLExecute failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }

        writeLog ('D', "The session data is stored to TIRDB");

        /* Clean up */
        err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
        if (error_s(statement, err, NULL)) {
            writeLog('W', "SQLFreeHandle failed");
        }

        DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
        return 0;
}

/*##**********************************************************************\
 *
 *      initialize_tirdb_for_benchmark
 *
 * Initialize TIRDB for one benchmark run. First figures out a new key
 * value for the benchmark (the table TEST_RUNS). Note that we don't use
 * automatic column values in TIRDB.
 *
 * Parameters:
 *      mode
 *          MODE_TO_TIRDB or MODE_TO_SQLFILE
 *
 *      ConnectString
 *          Connect string to TIRDB
 *
 *      resultFileName
 *          result filename
 * 
 *      bmrs 
 *          Pointer to the benchmark structure, which holds the information
 *          related to benchmarks to be run.
 *
 *      test_number
 *          index of the current benchmark.
 *
 *      tdf
 *          Pointer to the tdf structure, which holds tdf data
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int initialize_tirdb_for_benchmark(int mode,
                                   char *ConnectString,
                                   char *resultFileName,
                                   struct bmr *bmrs,
                                   int test_number,
                                   struct tdfs *tdf)
{
        SQLHENV tirdb_env; 
        SQLHDBC tirdb;
        SQLHSTMT statement;
        RETCODE err;
        SQLLEN ind = 0;
        SQLLEN nullData = SQL_NULL_DATA;
        SQLUSMALLINT ParameterNumber;
        /* A TIRDB column value to be initialized to zero */
        int initialZero = 0;
        int total_num_of_clients, j;
        /* Copy of the test run id stored to the struct tdfr */
        int test_run_id;
        FILE* fResults;
        
        if (mode == MODE_TO_SQLFILE) {
            createFileInSequence(&fResults, resultFileName);
        }
        
        if (mode == MODE_TO_TIRDB) {
            writeLog('D', "Storing initial test run data in TIRDB");
            if (ConnectDB(&tirdb_env, &tirdb, ConnectString, "TIRDB")) {
                return E_ERROR;
            }

            /* First get the test_run_id */
            err = SQLAllocHandle(SQL_HANDLE_STMT, tirdb, &statement);
            if (error_c(tirdb, err)) {
                writeLog('E', "SQLAllocHandle failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }

            /* Prepare a statement to get the maximum value of the key
               of the table TEST_RUNS */
            err = SQLPrepare(statement,
                             CHAR2SQL("SELECT MAX(test_run_id) FROM test_runs"),
                             SQL_NTS);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLPrepare failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindCol(statement,
                             1,
                             SQL_C_SLONG,
                             &test_run_id,
                             0,
                             &ind);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindCol failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLExecute(statement);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLExecute failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLFetch(statement);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLFetch failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLCloseCursor(statement);
            if (error_s(statement, err, NULL)) {
                writeLog('W', "SQLCloseCursor failed");
            }
            
            if (ind == SQL_NULL_DATA) {
                /* TEST_RUNS is empty, start with this key */
                test_run_id = 1;
            }
            else {
                /* New key is one more than max. key fetched */
                test_run_id++;
            }
            /* Store the new key */
            bmrs->test_run_id = test_run_id;
        }

        
        /* Count the total number of clients used in the run
           (all the remotes and local clients )*/
        total_num_of_clients =
            tdf->client_distributions[bmrs->client_distribution_ind].localLoad;
        
        for (j = 0; tdf->client_distributions[bmrs->client_distribution_ind].
                 rem_loads[j].remControls_index != 0; j++) {
            total_num_of_clients
                += tdf->client_distributions[bmrs->client_distribution_ind].
                rem_loads[j].remLoad;
        }

        if (mode == MODE_TO_TIRDB) {
            /* The test_run_id is determined. Store the
               benchmark data to TIRDB. Prepare the statement for
               storing the benchmark run row to TEST_RUNS */
            err = SQLPrepare(statement,
                             CHAR2SQL(TEST_RUN_INSERT),
                             SQL_NTS);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLPrepare failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            /* Bind the column values mainly from the bm
               (benchmark) structure */
            ParameterNumber = 1;
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_SLONG,
                                   SQL_INTEGER,
                                   0,
                                   0,
                                   &test_run_id,
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter test_run_id failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_SLONG,
                                   SQL_INTEGER,
                                   0,
                                   0,
                                   &(tdf->session_id),
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter session_id failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_CHAR,
                                   SQL_CHAR,
                                   TIRDB_TEST_NAME_LEN,
                                   0,
                                   bmrs->test_run_name,
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter test_name failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_DATE,
                                   SQL_DATE,
                                   0,
                                   0,
                                   &(bmrs->start_date),
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter start_date failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_TIME,
                                   SQL_TIME,
                                   0,
                                   0,
                                   &(bmrs->start_time),
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter start_time failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_DATE,
                                   SQL_DATE,
                                   0,
                                   0,
                                   &(bmrs->stop_date),
                                   0,
                                   &nullData);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter stop_date failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_TIME,
                                   SQL_TIME,
                                   0,
                                   0,
                                   &(bmrs->stop_time),
                                   0,
                                   &nullData);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter stop_time failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            /* test_completed to zero */
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_SLONG,
                                   SQL_INTEGER,
                                   0,
                                   0,
                                   &initialZero,
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter test_completed failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_SLONG,
                                   SQL_INTEGER,
                                   0,
                                   0,
                                   &test_number,
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter test_number failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindParameter(statement, /* Statement handle */
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_SLONG,
                                   SQL_INTEGER,
                                   0,
                                   0,
                                   &(total_num_of_clients),
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter rampup_time failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_SLONG,
                                   SQL_INTEGER,
                                   0,
                                   0,
                                   &(bmrs->warm_up_duration),
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter rampup_time failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_SLONG,
                                   SQL_INTEGER,
                                   0,
                                   0,
                                   &(bmrs->subscribers),
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter subscribers failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_SLONG,
                                   SQL_INTEGER,
                                   0,
                                   0,
                                   &initialZero,
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter perf_processed failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLExecute(statement);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLExecute failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLFreeHandle failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
        } else if (mode == MODE_TO_SQLFILE) {
            fwrite(TEST_RUN_INSERT, sizeof(char),
                   strchr(TEST_RUN_INSERT, '?') - TEST_RUN_INSERT, fResults);

            fprintf(fResults, "?, ?, '%s', '%d-%02d-%02d', '%02d:%02d:%02d', "
                    "NULL, NULL, 0, %d, %d, %d, %d, 0)",
                    bmrs->test_run_name, (&(bmrs->start_date))->year,
                    (&(bmrs->start_date))->month,
                    (&(bmrs->start_date))->day,
                    (&(bmrs->start_time))->hour,
                    (&(bmrs->start_time))->minute,
                    (&(bmrs->start_time))->second,
                    test_number,
                    total_num_of_clients,
                    bmrs->warm_up_duration,
                    bmrs->subscribers);
            fputs(";\n", fResults);
            fflush(fResults);
        }

        if (mode == MODE_TO_TIRDB) {
            /* Now the benchmark data is stored to table test_runs.
               Next store the data to transaction_mixes table */
            
            err = SQLAllocHandle(SQL_HANDLE_STMT, tirdb, &statement);
            if (error_c(tirdb, err)) {
                writeLog('E', "SQLAllocHandle failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLPrepare(statement,
                             CHAR2SQL(TRANSACTION_MIX_INSERT),
                             SQL_NTS);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLPrepare failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            /* Iterate through the transactions defined for
               the benchmark */
            for (j = 0;             
                 tdf->tr_mixes[bmrs->transaction_mix_ind].tr_props[j].transact[0]
                     != '\0'; j++) {
                ParameterNumber = 1;
                err = SQLBindParameter(statement,
                                       ParameterNumber++,
                                       SQL_PARAM_INPUT,
                                       SQL_C_SLONG,
                                       SQL_INTEGER,
                                       0,
                                       0,
                                       &test_run_id,
                                       0,
                                       NULL);
                
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLBindParameter test_run_id failed");
                    DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                    return E_ERROR;
                }
                
                err = SQLBindParameter(statement,
                                       ParameterNumber++,
                                       SQL_PARAM_INPUT,
                                       SQL_C_CHAR,
                                       SQL_CHAR,
                                       TIRDB_TRANSACTION_NAME_LEN,
                                       0,
                                       tdf->tr_mixes[bmrs->transaction_mix_ind].
                                       tr_props[j].transact,
                                       0,
                                       NULL);
                
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLBindParameter transaction_type failed");
                    DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                    return E_ERROR;
                }
                
                err = SQLBindParameter(statement,
                                       ParameterNumber++,
                                       SQL_PARAM_INPUT,
                                       SQL_C_SLONG,
                                       SQL_SMALLINT,
                                       0,
                                       0,
                                       &(tdf->tr_mixes[bmrs->
                                                       transaction_mix_ind].
                                         tr_props[j].prob),
                                       0,
                                       NULL);
                
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLBindParameter percentage failed");
                    DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                    return E_ERROR;
                }
                /* ... and store one transaction with the transaction
                   probability to the table TRANSACTION_MIXES */
                err = SQLExecute(statement);
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLExecute failed");
                    DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                    return E_ERROR;
                }
            }
            err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLFreeHandle failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            /* And finally, store the data to
               DATABASE_CLIENT_DISTRIBUTIONS table */
            err = SQLAllocHandle(SQL_HANDLE_STMT, tirdb, &statement);
            if (error_c(tirdb, err)) {
                writeLog('E', "SQLAllocHandle failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLPrepare(statement,
                             CHAR2SQL(CLIENT_DISTRIBUTION_INSERT),
                             SQL_NTS);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLPrepare failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            /* Store the database client distribution to TIRDB */
            /* First check if we used local clients */
            if (tdf->client_distributions[bmrs->
                                          client_distribution_ind].localLoad > 0) {
                ParameterNumber = 1;
                err = SQLBindParameter(statement,
                                       ParameterNumber++,
                                       SQL_PARAM_INPUT,
                                       SQL_C_SLONG,
                                       SQL_INTEGER,
                                       0,
                                       0,
                                       &test_run_id,
                                       0,
                                       NULL);
                
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLBindParameter test_run_id failed");
                    DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                    return E_ERROR;
                }
                
                err = SQLBindParameter(statement,
                                       ParameterNumber++,
                                       SQL_PARAM_INPUT,
                                       SQL_C_CHAR,
                                       SQL_CHAR,
                                       TIRDB_HOST_NAME_LEN,
                                       0,
                                       "localhost",
                                       0,
                                       NULL);
                
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLBindParameter transaction_type failed");
                    DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                    return E_ERROR;
                }
                
                err = SQLBindParameter(statement,
                                       ParameterNumber++,
                                       SQL_PARAM_INPUT,
                                       SQL_C_CHAR,
                                       SQL_CHAR,
                                       TIRDB_HOST_ADDRESS_LEN,
                                       0,
                                       "", /* no ip for localhost */
                                       0,
                                       NULL);
                
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLBindParameter transaction_type failed");
                    DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                    return E_ERROR;
                }
                
                err = SQLBindParameter(statement,
                                       ParameterNumber++,
                                       SQL_PARAM_INPUT,
                                       SQL_C_SLONG,
                                       SQL_INTEGER,
                                       0,
                                       0,
                                       &(tdf->client_distributions[
                                                 bmrs->client_distribution_ind].
                                         localLoad),
                                       0,
                                       NULL);
                
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLBindParameter percentage failed");
                    DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                    return E_ERROR;
                }
                err = SQLExecute(statement);
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLExecute failed");
                    DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                    return E_ERROR;
                }
            }
            /* Then iterate through the remote database client distributions
               defined for the benchmark */
            for (j = 0; tdf->client_distributions[
                         bmrs->client_distribution_ind].rem_loads[j].
                     remControls_index != '\0'; j++) {
                ParameterNumber = 1;
                err = SQLBindParameter(statement,
                                       ParameterNumber++,
                                       SQL_PARAM_INPUT,
                                       SQL_C_SLONG,
                                       SQL_INTEGER,
                                       0,
                                       0,
                                       &test_run_id,
                                       0,
                                       NULL);
                
                
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLBindParameter test_run_id failed");
                    DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                    return E_ERROR;
                }
                
                err = SQLBindParameter(statement,
                                       ParameterNumber++,
                                       SQL_PARAM_INPUT,
                                       SQL_C_CHAR,
                                       SQL_CHAR,
                                       TIRDB_HOST_NAME_LEN,
                                       0,
                                       remControls[
                                               tdf->client_distributions[
                                                       bmrs->client_distribution_ind].
                                               rem_loads[j].
                                               remControls_index].name,
                                       0,
                                       NULL);
                
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLBindParameter transaction_type failed");
                    DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                    return E_ERROR;
                }
                
                err = SQLBindParameter(statement,
                                       ParameterNumber++,
                                       SQL_PARAM_INPUT,
                                       SQL_C_CHAR,
                                       SQL_CHAR,
                                       TIRDB_HOST_ADDRESS_LEN,
                                       0,
                                       remControls[
                                               tdf->client_distributions[
                                                       bmrs->client_distribution_ind].
                                               rem_loads[j].
                                               remControls_index].ip,
                                       0,
                                       NULL);
                
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLBindParameter transaction_type failed");
                    DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                    return E_ERROR;
                }
                
                err = SQLBindParameter(statement,
                                       ParameterNumber++,
                                       SQL_PARAM_INPUT,
                                       SQL_C_SLONG,
                                       SQL_INTEGER,
                                       0,
                                       0,
                                       &(tdf->client_distributions[
                                                 bmrs->client_distribution_ind].
                                         rem_loads[j].remLoad),
                                       0,
                                       NULL);
                
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLBindParameter percentage failed");
                    DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                    return E_ERROR;
                }
                err = SQLExecute(statement);
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLExecute failed");
                    DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                    return E_ERROR;
                }
            }
        }

        if (mode == MODE_TO_SQLFILE) {
            writeLog ('D', "Initial benchmark data is stored to resultfile");
            fflush(fResults);
            fclose(fResults);
        } else {
            writeLog ('D', "Initial benchmark data is stored to TIRDB");
            
            /* Clean up */
            err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
            if (error_s(statement, err, NULL)) {
                writeLog('W', "SQLFreeHandle failed");
            }
            
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      finalize_tirdb_for_session
 *
 * Finalizes TIRDB for one session (TDF file) by updating the table
 * TEST_SESSIONS with the stop date and time.
 *
 * Parameters:
 *      ConnectString 
 *          Connect string to TIRDB
 *
 *      tdf 
 *          Pointer to the test definition structure,
 *          which holds all the information related
 *			to the current session.
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int finalize_tirdb_for_session(char *ConnectString, struct tdfs *tdf)
{
        SQLHENV tirdb_env;
        SQLHDBC tirdb;
        SQLHSTMT statement;
        RETCODE err;
        SQLUSMALLINT ParameterNumber;
        char msg[W_L];   /* Message buffer */
        
       
        sprintf(msg, "Finalizing session number %d", tdf->session_id);
        writeLog('I', msg);
        
        if (ConnectDB(&tirdb_env, &tirdb, ConnectString, "TIRDB")) {
            return E_ERROR;
        }
        
        err = SQLAllocHandle(SQL_HANDLE_STMT, tirdb, &statement);
        if (error_c(tirdb, err)) {
            writeLog('E', "SQLAllocHandle failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }
        
        /* Prepare the update statement */
        err = SQLPrepare(statement,
                         CHAR2SQL(TEST_SESSION_UPDATE),
                         SQL_NTS);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLPrepare failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }
        
        /* Bind the stop date and time parameters */
        ParameterNumber = 1;
        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_DATE,
                               SQL_DATE,
                               0,
                               0,
                               &(tdf->stop_date),
                               0,
                               NULL);
        
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter stop_date failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }
        
        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_TIME,
                               SQL_TIME,
                               0,
                               0,
                               &(tdf->stop_time),
                               0,
                               NULL);
        
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter stop_time failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }
            
        err = SQLBindParameter(statement,
                               ParameterNumber++,
                               SQL_PARAM_INPUT,
                               SQL_C_SLONG,
                               SQL_INTEGER,
                               0,
                               0,
                               &(tdf->session_id),
                               0,
                               NULL);
        
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindParameter session_id failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }
        
        err = SQLExecute(statement);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLExecute failed");
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            return E_ERROR;
        }
       
        writeLog ('D', "The session data is finalized to TIRDB");
        /* Clean up */
        err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
        if (error_s(statement, err, NULL)) {
            writeLog('W', "SQLFreeHandle failed");
        }            
        DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
        return 0;
}

/*##**********************************************************************\
 *
 *      finalize_tirdb_for_benchmark
 *
 * Finalizes TIRDB for one benchmark run by updating the table TEST_RUNS
 * and the columns stopDate and stopTime.
 *
 * Parameters:
 *      mode
 *          MODE_TO_TIRDB or MODE_TO_SQLFILE
 *
 *
 *      ConnectString
 *          Connect string to TIRDB
 *
 *      bmrs 
 *          Pointer to the benchmark structure,
 *          which holds the information related to
 *			the current benchmark.
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int finalize_tirdb_for_benchmark(int mode, char *ConnectString,
                                 char *resultFileName, struct bmr *bmrs)
{
        SQLHENV tirdb_env;
        SQLHDBC tirdb;
        SQLHSTMT statement;
        RETCODE err;
        SQLUSMALLINT ParameterNumber;
        char msg[W_L];   /* Message buffer */
        FILE* fResults = NULL;
        char *pPos;
        char *pOld;
        int i;

        if (mode == MODE_TO_SQLFILE) {
            openFileForWrite(&fResults, resultFileName);
        }
        
        if (mode == MODE_TO_TIRDB) { 
            sprintf(msg, "Finalizing test run number %d",
                    bmrs->test_run_id);
            writeLog('I', msg);
            
            if (ConnectDB(&tirdb_env, &tirdb, ConnectString, "TIRDB")) {
                return E_ERROR;
            }
            
            err = SQLAllocHandle(SQL_HANDLE_STMT, tirdb, &statement);
            if (error_c(tirdb, err)) {
                writeLog('E', "SQLAllocHandle failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            /* Prepare the update statement */
            err = SQLPrepare(statement,
                             CHAR2SQL(TEST_RUN_UPDATE),
                             SQL_NTS);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLPrepare failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            /* Bind the stop date, time and mqth_avg parameters */
            ParameterNumber = 1;
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_DATE,
                                   SQL_DATE,
                                   0,
                                   0,
                                   &(bmrs->stop_date),
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter stop_date failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_TIME,
                                   SQL_TIME,
                                   0,
                                   0,
                                   &(bmrs->stop_time),
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter stop_time failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_SLONG,
                                   SQL_INTEGER,
                                   0,
                                   0,
                                   &(bmrs->avg_mqth),
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter stop_time failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_SLONG,
                                   SQL_INTEGER,
                                   0,
                                   0,
                                   &(bmrs->test_run_id),
                                   0,
                                   NULL);
            
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindParameter test_run_id failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
            
            /* ... and update the database */
            err = SQLExecute(statement);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLExecute failed");
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
                return E_ERROR;
            }
        } else if (mode == MODE_TO_SQLFILE) {
            if (fResults != NULL) {
                /* update result file */            
                pOld = TEST_RUN_UPDATE;
                i = 0;
                do {
                    pPos = strchr(pOld, '?');
                    if (pPos == NULL) {
                        fwrite(pOld, sizeof(char), strchr(pOld,'\0') - pOld,
                               fResults);
                        fputs(";\n", fResults);
                    } else {
                        fwrite(pOld, sizeof(char), (pPos - pOld), fResults);
                        switch (i) {
                            case 0:
                                fprintf(fResults, "'%d-%02d-%02d'",
                                        (&(bmrs->stop_date))->year,
                                        (&(bmrs->stop_date))->month,
                                        (&(bmrs->stop_date))->day);
                                break;
                            case 1:
                                fprintf(fResults, "'%02d:%02d:%02d'",
                                        (&(bmrs->stop_time))->hour,
                                        (&(bmrs->stop_time))->minute,
                                        (&(bmrs->stop_time))->second);
                                break;
                            case 2:
                                fprintf(fResults, "%d", bmrs->avg_mqth);
                                break;
                            case 3:
                                fprintf(fResults, "?");
                                /* bmrs->test_run_id is zero */
                                /* in the future, could be given as
                                   cmd line parameter, though ?*/
                                break;
                        } 
                        pOld = pPos+1;
                        i++;
                    }
                } while (pPos != NULL);
                fflush(fResults);
            }
        }

        if (mode == MODE_TO_SQLFILE) {
            writeLog ('D', "The benchmark data is finalized to resultfile");
            fflush(fResults);
            fclose(fResults);
            moveFileInSequence(resultFileName);
        } else {
            writeLog ('D', "The benchmark data is finalized to TIRDB");        
            /* Clean up */
            err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
            if (error_s(statement, err, NULL)) {
                writeLog('W', "SQLFreeHandle failed");
            }            
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
        }
        
        return 0;
}

/*##**********************************************************************\
 *
 *      checkIfConfigurationExists
 *
 * Check if the target database configuration is stored to TIRDB.
 *
 * Parameters:
 *      tirdb
 *          TIRDB connect handle
 *
 *      ddf
 *          Pointer to the data definition structure,
 *
 *      new_configuration
 *          return value of 1, if this is a new configuration (
 *			the configuration was not found from TIRDB).
 *			Returns 0 otherwise.
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int checkIfConfigurationExists(SQLHDBC tirdb, struct ddfs *ddf,
                               int *new_configuration)
{
        RETCODE err;
        SQLHSTMT statement;
        int count = 0;
        char cmd[256];

        *new_configuration = 1;
        /* Form the database query for the configuration check */
        sprintf(cmd, "SELECT COUNT(*) FROM config_data WHERE "
                "config_id = '%s' AND config_name = '%s'",
                ddf->configuration_content_checksum,
                ddf->configuration_code );
        writeLog('D', cmd);

        err = SQLAllocHandle(SQL_HANDLE_STMT, tirdb, &statement);
        if (error_c(tirdb, err)) {
            writeLog('E', "SQLAllocHandle failed");
            return E_ERROR;
        }

        err = SQLPrepare(statement,
                         CHAR2SQL(cmd),
                         SQL_NTS);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLPrepare failed");
            return E_ERROR;
        }

        /* Bind the query result to the local variable 'count' */
        err = SQLBindCol(statement,
                         1,
                         SQL_C_SLONG,
                         &count,
                         0,
                         NULL);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLBindCol failed");
            return E_ERROR;
        }

        err = SQLExecute(statement);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLExecute failed");
            return E_ERROR;
        }
        err = SQLFetch(statement);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLFetch failed");
            return E_ERROR;
        }
        err = SQLCloseCursor(statement);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLCloseCursor failed");
        }

        /* Clean up */
        err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
        if (error_s(statement, err, NULL)) {
            writeLog('E', "SQLFreeHandle failed");
        }

        if (count > 1) {
            /* Something wrong with TIRDB. We should not have
               multiple rows with the same configuration ids */
            writeLog('W', "TIRDB table CONFIG_DATA has multiple "
                     "configurations with the same key!");
            *new_configuration = 0;
        }
        else if (count == 1) {
            /* The same configuration already exists in TIRDB */
            *new_configuration = 0;
        }
        else if (count == 0) {
            /* A new configuration */
            *new_configuration = 1;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      saveConfigurationFile
 *
 * Stores the target database configuration file to TIRDB if needed
 * (it is not stored if the same configuration file with the same conf
 * id is already earlier stored to TIRDB).
 *
 * Parameters:
 *      tirdb
 *          TIRDB connect handle
 *
 *      ddf
 *          Pointer to the data definition structure,
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int saveConfigurationFile(SQLHDBC tirdb, struct ddfs *ddf)
{
        SQLHSTMT statement;
        RETCODE err;
        SQLUSMALLINT ParameterNumber;
        int new_configuration;

        /* First check if the same configuration has been already
           stored to TIRDB earlier */
        checkIfConfigurationExists(tirdb, ddf, &new_configuration);
        
        if (new_configuration) {
            /* Store the target database configuration name, the
               configuration file content and a hash key calculated
               from the file content to TIRDB */
            writeLog('I', "Saving new target DB configuration to TIRDB");
            
            err = SQLAllocHandle(SQL_HANDLE_STMT, tirdb, &statement);
            if (error_c(tirdb, err)) {
                writeLog('E', "storeResult/SQLAllocHandle failed");
                return E_ERROR;
            }
            
            err = SQLPrepare(statement,
                             CHAR2SQL("INSERT INTO config_data "
                                      "(config_id,config_name,config_file,"
                                      "config_comments) VALUES (?,?,?,?)"),
                             SQL_NTS);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "storeResult/SQLPrepare config_data failed");
                return E_ERROR;
            }
            
            /* Bind the data from the ddf strucure */
            ParameterNumber = 1;
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_CHAR,
                                   SQL_CHAR,
                                   TIRDB_CONFIG_ID_LEN,
                                   0,
                                   ddf->configuration_content_checksum,
                                   0,
                                   NULL);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindparameter config_id to "
                         "config_data failed");
                return E_ERROR;
            }
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_CHAR,
                                   SQL_CHAR,
                                   TIRDB_CONFIG_NAME_LEN,
                                   0,
                                   ddf->configuration_code,
                                   0,
                                   NULL);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindparameter configuration_code to "
                         "config_data failed");
                return E_ERROR;
            }
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_CHAR,
                                   SQL_VARCHAR,
                                   TIRDB_CONFIG_FILE_LEN,
                                   0,
                                   ddf->configuration_file_contents,
                                   strlen(ddf->configuration_file_contents),
                                   NULL);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindparameter configuration_file_contents "
                         "to config_data failed");
                return E_ERROR;
            }
            err = SQLBindParameter(statement,
                                   ParameterNumber++,
                                   SQL_PARAM_INPUT,
                                   SQL_C_CHAR,
                                   SQL_CHAR,
                                   TIRDB_CONFIG_COMMENTS_LEN,
                                   0,
                                   ddf->configuration_comments,
                                   0,
                                   NULL);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "SQLBindparameter configuration_comments "
                         "to config_data failed");
                return E_ERROR;
            }

            /* Execute the statement */
            err = SQLExecute(statement);
            if (error_s(statement, err, NULL)) {
                writeLog('E', "storeResult/SQLExecute config_data failed");
                return E_ERROR;
            }

            /* Clean up */
            err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
            if (error_s(statement, err, NULL)) {
                writeLog('W', "SQLFreeHandle failed");
            }
        }
        return 0;
}
