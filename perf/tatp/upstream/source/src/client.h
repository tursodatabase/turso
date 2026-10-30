/**********************************************************************\
**  source       * client.h
**  description  * The purpose of a client is to feed
**		           transactions to the target database. The
**		           transactions are selected randomly from a
**		           pre-defined set of transactions based on the
**		           probability distribution of the transactions
**		           (defined in a TDF file).
**
**  	     	   The user defined number of instances of the client
**		           are started for each benchmark run to feed
**		           transactions to the target database.
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public 
**  License 1.0 as published by the Open Source Initiative (OSI). 
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifndef CLIENT_H
#define CLIENT_H

#include "const.h"
#include "communication.h"
#include "targetdb.h"

#ifdef WIN32
/* Windows header files */
#include <windows.h>
#include <limits.h>
#else
/* UNIX header files */
/* In case of other operating systems, use OS specifiec directives
   (specified in the MAKE file) and an OS specific header */
#include <unistd.h>
#include <stdlib.h>
#endif

/* common header files*/
#include <sys/timeb.h>
#include "util.h"
#include "timer.h"

#include "tatp.h"

#include <time.h>
#include <string.h>

/* TATP-specific functions (to set the coulmn values of the TATP tables) */
#include "columnvalues.h"

/* Error code for Solid TF status */
#define E_SSAC_CONNECT_EXPECT_ROLLBACK 25216 
 
#define NAME_LENGTH 64
/* CLIENT_IDLE is a pause in milliseconds each client
   waits after connecting to the target database */
#define CLIENT_IDLE 1000
#define READ_BUFFER_SIZE 1024
#define ERROR_CODE_LENGTH 6

/* commit block size */
#define COMMITBLOCK_SIZE 1

/* Maximum test length client/statistics can handle is given
   in seconds (86400 seconds = 24 hours */
#define MAX_TEST_LENGTH_CLIENT 86400

#ifndef NO_TPS_TABLE
/* TPS table object - statemenst and variables */
typedef struct TPStable {
        SQLHSTMT    stmtUpdate;     /* statement handle for update */
        int totalTPSValue;	        /* tps value per client per timeslot */
        int db1;		            /* transactions committed at db1 */
        int db2;		            /* transactions committed at db2 */
        int clientID;
} tpstable_t;
#endif

/* enumeration of different functions used in the transaction definitions */
enum funcname_e {
	e_rnd,
	e_rndstr,
	e_getvalue,
	e_value,
	e_bindcol
};

/* The transaction file parser modes */
enum mode_e {
	SEEK_FOR_TRANSACTION,
	IN_TRANSACTION
};

typedef struct client_t client_t;
typedef struct transaction_t transaction_t;

/* function call struct inside a sql clause struct */
typedef struct functioncall_st {
        /* pointer to next function call */
        struct functioncall_st *func_next;
        /* function pointer */
        long (*func_function)(client_t *, const char*, char*);    
        enum funcname_e function_name;        /* name of the function */
        char func_type[NAME_LENGTH];          /* function return value type */
        /* global name 'to which to store' or 'from which to load' the value */
        char func_var_name[NAME_LENGTH];
        void* func_var_value;                 /* variable value */
	int func_var_value_num;
} func_t;

/* sql clause struct inside a transaction struct */
typedef struct sql {
        /* pointer to next sql clause (within a transaction) */
        struct sql *sql_next;
        /* SQL clause with '?' placeholders */
        char *sql_clause;
        /* statement handle for this sql clause */
        SQLHSTMT sql_statement;
        /* pointer to list of functions contained in this sql clause */
        struct functioncall_st *sql_func_list;
#ifdef _DEBUG
        int sql_ok_amount;                  /* how many were executed OK */
        int sql_no_rows_amount;             /* how many returned 0 rows */
#endif
} sql_t;

/* the struct of accepted errors inside a transaction struct */
typedef struct sqlerror {
        struct sqlerror *error_next;        /* pointer to next error code */
        char error_code[ERROR_CODE_LENGTH]; /* error code */
#ifdef _DEBUG
        int error_amount;                   /* how many errors of this kind */
#endif
} sqlerror_t;

/* transaction struct */
typedef struct transaction {
        struct transaction *trans_next ;   /* pointer to next in list */
        char trans_name[NAME_LENGTH];      /* name of the transaction */
        struct sql *trans_sql;    	       /* pointer to linked list of 
                                              SQL clauses */
        /* pointer to linked list of accepted errors */
        struct sqlerror *trans_errors;
        /* response time statistics -> a slot for each millisecond value */
        int response_times[MAX_RESP_TIME_SLOTS];
        /* number of commits */
        int commitCount;
        /* number of rollbacks */
        int rollbackCount;
        /* number of transaction ignored in result data */
        int ignoredCount;
#ifdef LATENCYBOUNDS
        int maxLatency;
        int minLatency;
#endif
} trans_t;

/* global variable list struct. Global variables are defined
   inside a transaction definition and they are valid only
   inside that transaction. */
typedef struct variable {
        struct variable *var_next;      /* pointer to next variable */
        char var_name[NAME_LENGTH];     /* variable name */
        void* var_value;                /* variable value */
        int var_value_num;
} var_t;

typedef struct connection_t
{
        SQLHDBC hdbc;
#ifndef NO_TPS_TABLE
/* TPS table object */
        tpstable_t tps;		            
#endif
        dbtype db;
#ifdef TC_COUNT
        char solid_connection_one[256];
        char solid_connection_two[256];
#endif
} connection_t;

/* core functions that are involved when parsing the SQL file */
int readSQLFile(client_t* , FILE* );
int buildSQLRecord(char* clause, char** newclause,
                   sql_t* sqlrecord);
void storeGlobalVariable(client_t* client, func_t* bind);

/* Initializes the socket communication system */
int initComm(client_t* , char *name, int port);

/* Initialize target DB for population action */
int initializeDBpopulation(SQLHDBC testdb, char* DBSchemaFileName,
                           char* DBSchemaName);

/* function that waits for a specific message */
int waitForControlMessage (client_t* , SOCKET sck, 
                           int* previous_control_message);

/* function that sends messages to statistics module */
int sendToStatistics (SOCKET sck, int messageType, struct message_dataS *mdata,
                      int clientID, int fatalCount);

/* function that collects results and sends them */
int sendResults(client_t*, SOCKET sck);

#ifdef WIN32
/* The rampup start time */
struct _timeb rampup_start_time;
/* The test start time */
struct _timeb test_start_time;
/* The latest change point of the mqth time slot */
struct _timeb latest_mqth_start_time;
#else
/* The rampup start time */
struct timeval rampup_start_time;
/* The test start time */
struct timeval test_start_time;
/* The latest change point of the mqth time slot */
struct timeval latest_mqth_start_time;
#endif

/* check which MQTH slot to use */
int checkAndUpdateMQTHSlot();

/* cleaning function */
int finalize();

static int isAcceptedError(trans_t* tr, SQLHSTMT hstmt);

static int checkError(SQLRETURN   rc,
		      SQLSMALLINT handleType,
		      SQLHANDLE   handle,
		      const char* tr_name,
		      const char* location,
		      transaction_t* transaction);

static int checkErrorS(SQLRETURN rc,
		       SQLHSTMT hstmt,
		       const char* tr_name,
		       const char* location,
		       transaction_t* transaction);

#define MAX_STRLEN     255
#define STRODBCRET(v)  (v == SQL_SUCCESS ? "SQL_SUCCESS" : \
                       (v == SQL_SUCCESS_WITH_INFO ? "SQL_SUCCESS_WITH_INFO" : \
                       (v == SQL_NO_DATA_FOUND ? "SQL_NO_DATA_FOUND" : \
                       (v == SQL_ERROR ? "SQL_ERROR" : \
                       (v == SQL_INVALID_HANDLE ? "SQL_INVALID_HANDLE" : \
                       (v == SQL_STILL_EXECUTING ? "SQL_STILL_EXECUTING" : \
                       (v == SQL_NEED_DATA ? "SQL_NEED_DATA" : "UNKNOWN_VAL") \
                        ) ) ) ) ) )
void get_diagnostics(SQLSMALLINT handletype,
		     SQLHANDLE   handle,
		     int*        native_err,
		     char*    sql_state);

int doRollback(SQLHDBC hdbc, const char* tr_name, int* failover);
int rollbackOnFailover(SQLHDBC hdbc, const char* tr_name, client_t* client);
static int prepareTransaction(transaction_t* db_transaction, 
                              trans_t* transaction);

static int runTransaction(client_t* client,
                          trans_t* transaction,
                          struct timertype_t* transaction_timer,
                          __int64* transaction_time,
                          int* rollbackOnError,
                          int* acceptedError,
                          transaction_t* db_transaction);

static void closeStatements(client_t* client);

RETCODE setupTargetDB (SQLHDBC* testdb, connection_t* connection, 
                       client_t* client);

#ifndef NO_TPS_TABLE
int prepareUpdateTPSrow (tpstable_t* tps, int solid_ha_stat,
                         char *DBSchemaName);
int insertTPSrow(SQLHDBC hdbc, int clientID, char *DBSchemaName);
static int deleteTPSrow(connection_t* connection, int clientID,
                            char *DBSchemaName);
int updateRealtimeStats(client_t* client, int throughput, int db1_mqth,
                        int db2_mqth);
#endif

int runTests(client_t* client,
             char   *transactions[],
             int    tr_choices,
             int    warmup_time,
             int    run_time,
             char   *DBSchemaName,
             int    operation_mode);

/* Special message function for the client module. */
/* This differs from the 'message' function defined
in 'util' by adding client->statistics communication */
void message_client(char errortype,const char *tr_name,const char *errortext);

/* functions and placeholders, some of these can be called
from inside the transactions */
int getvalue(client_t *, func_t *parameter);

#endif /* CLIENT_H */
