/**********************************************************************\
**  source       * client.c
**
**  description  * The purpose of a client is to feed
**                 transactions to the target database. The
**                 transactions are selected randomly from a
**                 pre-defined set of transactions based on the
**                 probability distribution of the transactions
**                 (defined in a Test Definition File).
**
**                 The user defined number of instances of the client
**                 are started for each benchmark run to feed
**                 transactions to the target database.
**
**                 A client is the only program module that
**                 communicates with the target database.
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#include "client.h"
#include "random.h"
#include "thd.h"
#include "server.h"

#include <assert.h>
#include <math.h>

#ifdef WIN32
#include <windows.h>
#else
#include <time.h>
#include <sys/time.h>
#endif

#define FAILOVER_RETRIES 20   /* number of rollback retries after failover */

THD_RET THD_FUN client_thread(
    THD_ARG arg);

struct client_t
{
        cmd_type operation_mode;          /* type of run */
        struct timertype_t global_timer;  /* Timer variable */
        int testTimeOffset;               /* offset is in milliseconds */
        var_t *variables;                 /* global variable table */
        trans_t *tr_head;                 /* transaction list */
        int clientID;                     /* client ID for communication */
        int errorCount;                   /* number of errors */
        int fatalCount;
        int listenPort;                   /* TCP port to use */
        int *throughput_data;             /* array of throughput value per
                                             time unit (which will be defined
                                             by throughput_resolution) */
        int current_mqth_time_slot;       /* The number of the current mqth slot */
        connection_t connection;
        int solid_ha_stat;
        log_t log;
        rand_t rand;
        communication_t comm;
        thd_thread_t thread;        
};

struct transaction_t
{
        const connection_t* connection;
        int failover;
};

static void startTransaction(
        transaction_t* transaction,
        const connection_t* connection);

static int endTransaction(
        transaction_t* transaction,
        int transaction_status,
        const char* tr_name,
        const char* location,
        struct timertype_t* commit_timer,
        client_t* client);

static int g_argc;
static char** g_argv;

static log_t* g_log = NULL;

static int testID = 0;                  /* test ID */
static int popl_size = 0;               /* amount of subscribers in the test database */

extern int min_subs_id;
extern int max_subs_id;

static int clientProcessID;             /* id of this client process */
static int firstClientID;               /* id of the first client thread */
static int numberOfClientThreads;       /* threads in this process */

static int nurand_sid_a = 0;
static int throughput_resolution = 0;   /* time resolution to use
                                           in calculations, in seconds */
static int num_of_time_slots = 0;
static int uniform;
static int connectPortControl = CONTROL_PORT;

static int serial_keys = 0;
static int commitblock_size = 1;

#ifndef NO_TPS_TABLE
static int reportTPS = 1;
#endif
static int detailedStatistics = 0;

#ifndef LINEAR_RESPONSE_SCALE
static int *response_time_bounds = NULL;
static int response_time_bound_count = 0;
#endif

static char init_sql_file[FILENAME_LENGTH] = {'\0'};

/* per-thread global variables */
static thd_tls_t g_client_tls;
static int g_server_count = 0;          /* a number of times a server
                                           has been started */
static thd_mutex_t g_server_count_mtx;  
static struct server_t* g_server;       /* server start handle */
#ifndef OLD_FD
static thd_mutex_t comm_mutex;
#endif

/*##**********************************************************************\
 *
 *      getLogObject
 *
 * Returns pointer to log object
 *
 * Parameters:
        none
 * Return value
 *      pointer to log object
 */
log_t* getLogObject( )
{
        if (g_log == NULL) {
            client_t* client = (client_t*) thd_tls_get(&g_client_tls);
            return &client->log;
        } else {
            return g_log;
        }
}

#ifdef TC_COUNT
/*##**********************************************************************\
 *
 *      trim_connect_string
 *
 * Remove leading and trailing spaces, otherwise, compress to one space;
 *  change to lower case
 *
 * Parameters:
 *      string to edit
 *
 * Return value:
 *      none
 */
void trim_connect_string(char *s)
{
    char *ws = s;
    int wasspace = 1; /*remove next space*/
#ifdef _DEBUG
    char *start = s;
    char debug[256];
    sprintf(debug, "Before trim: %s<%d", ws, strlen(ws));
    message_client('D', NULL, debug);
#endif

    while (*s != '\0') {
        *s = tolower(*s);
        if (isspace(*s)) {
            if (wasspace) {
                s++;
                continue;
            } else {
                wasspace = 1;
            }
        }
        else {
            wasspace = 0;
        }

        *ws = *s;
        ws++;
        s++;

    }
    *ws = '\0';
    if (ws != s) {
        /* recursively remove trailing spaces */
        ws--; /*skip the null terminator*/
        while (isspace(*ws)) {
            *ws = '\0';
            ws--;
        }
    }
#ifdef _DEBUG
    sprintf(debug, "After trim:  %s<%d", start, strlen(start));
    message_client('D', NULL, debug);
#endif
}

#endif

/*##******************************************************\
 *
 *      main
 * 
 *		Extracts the command arguments, initializes the
 *		communication with the statistics module, calls
 *		routines to parse the transaction file, connetcs
 *		to the target database, does some initialization
 *		on the target database and finally starts the benchmark
 *		by calling runTests().
 *
 * Parameters :
 *      argc
 *          Number of arguments
 *      argv
 *          Arguments as an array. The following lists all
 *          the arguments passed to a client.
 *
 *  argv[0] = program name (= client)
 *  argv[1] = Transaction file name to open
 *  argv[2] = Connection init file name to open
 *  argv[3] = DB Schema file name
 *  argv[4] = DB Schema name
 *  argv[5] = Operation type
 *  argv[6] = test database connect string
 *  argv[7] = serial keys boolean
 *  argv[8] = commitblock size
 *  argv[9] = check targetdb boolean
 *  argv[10] = ramp_up time (in minutes)
 *  argv[11] = test time (in minutes), including ramp-up time
 *  argv[12] = logging verbosity
 *  argv[13] = throughput resolution (in seconds)
 *  argv[14] = statistics module IP address
 *  argv[15] = client ID (universally unique id per test session)
 *  argv[16] = TCP port number to listen to
 *  argv[17] = TCP port number of the control module
 *  argv[18] = test ID
 *  argv[19] = subscriber table population size
 *  argv[20] = min used subscriber id
 *  argv[21] = max used subscriber id
 *  argv[22] = uniform
 *  argv[23] = number of clients
 *  argv[24] = report TPS value
 *  argv[25] = report detailed statistics
 *  argv[22] = transaction name
 *  argv[23] = transaction probability
 *    <... name-probability-pairs...>
 *  argv[n-1] = transaction name
 *  argv[n] = probability of transaction argv[n-1]
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int main(int argc, char *argv[])
{
        int       err = 0, ret = 0;
        FILE      *fSQL;          /* Transaction file to open */
        int       i;
        int       listenPort = 0;     /* TCP port to use */
        client_t* clients;
        int       check_targetdb;
        char      DBSchemaFileName[FILENAME_LENGTH] = {'\0'};
        char      DBSchemaName[W] = {'\0'};       
        cmd_type  operation_mode;
        char      msg[W_L];
        char      workdir[W_L];
        char      errtxt_buf[256];

        SQLHENV   testdb_env = SQL_NULL_HENV;
        SQLHDBC   testdb = SQL_NULL_HDBC;

        dbtype db_type;
        char version[W] = {'\0'};
        
#ifdef _DEBUG
        char      debug[256];
        /* For TATP software performance analysis
           -> not needed in actual benchmark runs */
        initializeTiming();
#endif        
        /* Initialize the communication system */
        err = initializeCommunicationGlobal();
        if (err) {
            printf("Cannot initialize the communication system\n");
            return -1;
        }

        fSQL = 0;

        /* check arguments */
        if ( (argc < CLIENT_STATIC_ARGC )
            || ( (argc - CLIENT_STATIC_ARGC)%2 != 0) )
        {
            /* This should not actually happen */
            printf("Client error: Wrong number of arguments...exiting\n");
            return -1;
        }

        g_argc = argc;
        g_argv = argv;

        /* collect the arguments */
        firstClientID = atoi(argv[ARGV_CLIENT_ID]);
        listenPort = atoi(argv[ARGV_CLIENT_TCP_LISTEN_PORT]);
        connectPortControl = atoi(argv[ARGV_CONTROL_TCP_LISTEN_PORT]);
        testID = atoi(argv[ARGV_TEST_ID]);
        popl_size = atoi(argv[ARGV_POPULATION_SIZE]);

        min_subs_id = atoi(argv[ARGV_MIN_SUBS_ID]);
        max_subs_id = atoi(argv[ARGV_MAX_SUBS_ID]);

        serial_keys = atoi(argv[ARGV_SERIAL_KEYS]);
        commitblock_size = atoi(argv[ARGV_COMMITBLOCK_SIZE]);

        check_targetdb = atoi(argv[ARGV_CHECK_TARGETDB]);
        
#ifndef NO_TPS_TABLE
        reportTPS = atoi(argv[ARGV_REPORT_TPS]);
#endif
        detailedStatistics = atoi(argv[ARGV_DETAILED_STATISTICS]);
        uniform = atoi(argv[ARGV_UNIFORM]);
        numberOfClientThreads = atoi(argv[ARGV_NUM_CLIENT_THREADS]);
        clientProcessID = atoi(argv[ARGV_CLIENT_PROCESS_ID]);
        
        operation_mode = atoi(argv[ARGV_OPERATION_TYPE]);

        /* initialize throughput bookkeeping variables */
        throughput_resolution = atoi(argv[ARGV_THROUGHPUT_RESOLUTION]);
        num_of_time_slots = (MAX_TEST_LENGTH_CLIENT / throughput_resolution)+1;

        /* change working directory if needed */
        if (strcmp(argv[ARGV_WORKDIR], ".") != 0) {
            sprintf(workdir, "%s%d", argv[ARGV_WORKDIR], clientProcessID);
            CH_CWD(workdir);
        }
        
        g_log = (log_t*) malloc(sizeof (log_t));
        /* initialize logging */
        sprintf(errtxt_buf, "CLIENT%d", firstClientID);
        initializeLog(atoi(argv[ARGV_LOG_VERBOSITY]), errtxt_buf, 6);
        sprintf(errtxt_buf, "client%d.log", firstClientID);
        if (createLog(errtxt_buf) != 0) {
            /* cannot continue if log file failed */
            message_client('F', NULL, "Error initializing log file..."
                           "exiting.");
            return -1;
        }
        if (strcmp(argv[ARGV_WORKDIR], ".") != 0) {
            sprintf(msg, "Setting client working directory to '%s'", workdir);
            message('I', msg);
        }

#ifdef _DEBUG
        /* Write out all arguments (debug only) */
        for (i = 0; i < argc; i++) {
            sprintf(debug, "argv[%d] = %s", i, argv[i]);
            message_client('D', NULL, debug); 
        }
#endif

        if (min_subs_id == 0 && max_subs_id == 0) {
            /* operate on "full range" (IDs 1 - n) of subscribers */
            min_subs_id = 1;
            max_subs_id = popl_size;
        }
        if (uniform) {
            nurand_sid_a = 0;
        } else {
            if ( (max_subs_id - min_subs_id + 1) <= 1000000) {
                nurand_sid_a = 65535;
            } else if ( (max_subs_id-min_subs_id + 1) <= 10000000) {
                nurand_sid_a = 1048575;
            } else {
                nurand_sid_a = 2097151;
            }
        }

#ifndef LINEAR_RESPONSE_SCALE
        {

            const int decimal_steps = LOG_RESP_TIME_SLOTS_PER_DECADE;
            
            double log_base = pow(10.0, 1.0 / decimal_steps);
            double min_bound = LOG_RESP_TIME_MIN_BOUND;   /* microseconds */
            /* int decades = LOG_RESP_TIME_DECADES; */
            /* until 1 sec */
            double bound;
            int i;

            response_time_bound_count = MAX_RESP_TIME_SLOTS;
            response_time_bounds = (int*) malloc(sizeof(int) *
						 response_time_bound_count);

            if (response_time_bounds == NULL) {
                message('F', "Cannot reserve memory for response time "
                        "histogram");
                ret = E_FATAL;
                goto cleanup;
            }

            if (err == 0) {
                bound = min_bound;
                for (i = 0; i != response_time_bound_count; ++i) {
                    /* microseconds */
                    response_time_bounds[i] = (int) floor(bound + 0.5);
                    bound *= log_base;
                }
            }
        }
#endif
        /* start local server if needed */
        if (err == 0) {
            err = startServer(&g_server);
            if (err != 0) {
                sprintf(msg, "Could not start database server (%s), error %d",
                    server_name,
                    err);
                message('F', msg);
                ret = E_FATAL;
                goto cleanup;
            } else {
                g_server_count = 1;
            }
        }
        
        /* modify schema filename */
        /* '.' indicates that no targetdb schema file was given */
        strncpy(DBSchemaFileName, argv[ARGV_DBSCHEMAFILENAME], FILENAME_LENGTH);
        if (DBSchemaFileName[0] == '.') {
            DBSchemaFileName[0] = '\0';
        }

        /* modify connection_init_filename */
        strncpy(init_sql_file, argv[ARGV_CONNECTION_INIT_SQL_FILENAME], FILENAME_LENGTH);
        if (init_sql_file[0] == '.') {
            init_sql_file[0] = '\0';
        }        

        /* modify schema name */
        strncpy(DBSchemaName, argv[ARGV_DBSCHEMANAME], W);
        if (DBSchemaName[0] == '.') {
            DBSchemaName[0] = '\0';
        }
        
        /* check database schema and population */
        if (operation_mode == RUN || operation_mode == RUN_DEDICATED) {
            if (min_subs_id == 1 && max_subs_id == popl_size) {
                /* check only when RUN in a single non-partitioned database */
                if (check_targetdb) {
                    if (testdb == SQL_NULL_HDBC) {
                        if (ConnectDB(&testdb_env, &testdb, 
                                      argv[ARGV_TEST_DSN],
                                      "target database") ) {
                            message_client('F', "ConnectDB()", "ConnectDB failed!");
                            ret = E_FATAL;
                            goto cleanup;
                        }
                    }
                    db_type = DB_GENERIC;
                    detectTargetDB (&testdb, &db_type, version, 1);
                    if (checkTargetDatabase(operation_mode,
                                            testdb,
                                            popl_size,
                                            DBSchemaName)) {
                        ret = E_FATAL;
                        goto cleanup;
                    }
                }
            }            
        } else if (operation_mode == POPULATE_CONDITIONALLY) {
            if (check_targetdb) {
                if (testdb == SQL_NULL_HDBC) {
                    if (ConnectDB(&testdb_env, &testdb, 
                                  argv[ARGV_TEST_DSN],
                                  "target database") ) {
                        message_client('F', "ConnectDB()", "ConnectDB failed!");
                        ret = E_FATAL;
                        goto cleanup;
                    }
                }       
                if (checkTargetDatabase(operation_mode,
                                        testdb,
                                        popl_size,
                                        DBSchemaName)) {
                    operation_mode = POPULATE;
                } else {
                    /* database was OK, no need to run anything in the threads */
                    operation_mode = NOP;
                }
            } else {
                operation_mode = POPULATE;
            }
        } 

        if (operation_mode == POPULATE || operation_mode == POPULATE_INCREMENTALLY) {
            if (testdb == SQL_NULL_HDBC) {
                if (ConnectDB(&testdb_env, &testdb, 
                              argv[ARGV_TEST_DSN],
                              "target database") ) {
                    message_client('F', "ConnectDB()", "ConnectDB failed!");
                    /*ret = E_FATAL;
                      goto cleanup;*/
                    operation_mode = NOP;
                }
            }
            if (operation_mode == POPULATE) {
                if (initializeDBpopulation(testdb,
                                           DBSchemaFileName,
                                           DBSchemaName)) {
                    message_client('F', "initializeDBpopulation()",
                                   "initializeDBpopulation failed!");
                    operation_mode = NOP;
                    /*ret = E_FATAL;
                      goto cleanup;*/                    
                }
            } else {
                operation_mode = POPULATE;
            }
            
        }
        
        if (testdb != SQL_NULL_HDBC) {            
            if (DisconnectDB(&testdb_env, &testdb, "target database")) {
                message_client('F', "main()", "DisconnectDB failed!");
            } else {
                message_client('D', "main()", "DisconnectDB succeeded");
            }
        }

        /* Logging will continue in threads */        
        finalizeLog();
        free(g_log);
        g_log = NULL;

        if (err == 0) {
            thd_tls_create(&g_client_tls);
            thd_mutex_create(&g_server_count_mtx);

#ifndef OLD_FD
            thd_mutex_create(&comm_mutex);
#endif
            g_server_count = numberOfClientThreads;
            clients = (client_t*) malloc(sizeof(client_t) * numberOfClientThreads);

            for (i = 0; i != numberOfClientThreads; ++i) {
                client_t* client = clients + i;
                client->operation_mode = operation_mode;                
                client->clientID = firstClientID + i;
                client->listenPort = listenPort + i;
                client->testTimeOffset = 0;     /* offset is in milliseconds */
                client->tr_head = NULL;         /* transaction list */
                client->variables = NULL;
                client->errorCount = 0;         /* number of errors */
                client->fatalCount = 0;
                client->current_mqth_time_slot = 0;  /* The number of the
                                                        current mqth slot */
                client->solid_ha_stat = 0;
                thd_thread_init(&client->thread);
                thd_thread_start(&client->thread, client_thread,
				 (THD_ARG) client);
            }
            for (i = 0; i != numberOfClientThreads; ++i) {
                client_t* client = clients + i;
                thd_thread_join(&client->thread);
            }
            if (clients != NULL) {
                free(clients);
            }
            thd_mutex_destroy(&g_server_count_mtx);
#ifndef OLD_FD
            thd_mutex_destroy(&comm_mutex);
#endif            
            thd_tls_destroy(&g_client_tls);
        }
        
cleanup:
        if (g_server_count > 0) {
            err = stopServer(g_server);
            if (err != 0) {                
                char msg[W_L];
                sprintf(msg, "Could not stop database server (%s), "
                        "error %d",
                        server_name,
                        err);
                message_client('E', NULL, msg);
            }
        }
        if (g_log != NULL){ 
            finalizeLog();            
            free(g_log);
        }
#ifndef LINEAR_RESPONSE_SCALE
        free(response_time_bounds);
#endif
        if (ret) {
            return ret;
        } else {
            return 0;
        }
}

/*##**********************************************************************\
 *
 *      initializeDBpopulation
 *
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int initializeDBpopulation(SQLHDBC testdb, char* DBSchemaFileName,
                           char* DBSchemaName)
{
        char      txt_buf[W_L];
        int       err;
        
        if (*DBSchemaFileName != 0) {
            /* process DB schema file */
            sprintf(txt_buf, "Target DB schema file '%s'",
                    DBSchemaFileName);
            message_client('I', "initializeDBpopulation()", txt_buf);
            err = initializeTargetDatabase(testdb,
                                           DBSchemaFileName,
                                           DBSchemaName);
            if (err){
                message('E', "Error while initializing the target database");
                return E_FATAL;
            }
        }
        /* check that all the tables and fields exist */
        err = checkTableSchema(testdb, DBSchemaName);
        if (err) {
            message('E', "Target database table schema is invalid.");
            /* This is always a fatal situation */
            return E_FATAL;
        }
       
        /* empty the TATP tables */
        err = emptyTATPTables(testdb, DBSchemaName);
        if (err == E_FATAL) {
            message('E', "Failed to empty TATP tables.");
            return err;
        }
        
        return E_OK;
}



/*##**********************************************************************\
 *
 *      client_thread
 *
 * Client thread to run
 *
 * Parameters:
 *      arg 
 *          thread argument
 *
 * Return value:
 *      thread return value
 */
THD_RET THD_FUN client_thread(THD_ARG arg)
{
        int       argc;
        char**    argv;
        SQLHENV   testdb_env = SQL_NULL_HENV;
        SQLHDBC   testdb = SQL_NULL_HDBC;
        int       err = 0, ret = 0;
        char      errtxt_buf[256];
        int       capability = 0;
        FILE      *fSQL;              /* Transaction file to open */
        int       i;
        int       last_control_message = MSG_OK;
        RETCODE   sqlerr;
        SOCKET    sckStatistics;      /* Communication socket to statistics
                                         module */
        SOCKET    sckControl;         /* Communication socket to control
                                         module*/
        char      DBSchemaName[W] = {'\0'};
        
        client_t* client = (client_t*) arg;
        connection_t* connection = &client->connection;

        /* needed for populate commands only */
        int client_min_s_id = 0;
        int client_max_s_id = 0;
        int subscribers_client = 0;
#ifdef WIN32
        LARGE_INTEGER freq;
        LARGE_INTEGER ticks;
#else
        struct timeval timev;
#endif

#ifdef ACCELERATOR
#ifdef CACHE_MODE
        char c;
#endif        
#endif
        argc = g_argc;
        argv = g_argv;

        thd_tls_set(&g_client_tls, (THD_TLS_VALUE_T) client);

        initTimer(&client->global_timer, TIMER_MILLISECOND_SCALE);
        client->throughput_data = (int*) malloc(sizeof(int)
                                                * num_of_time_slots);
        for (i = 0; i < num_of_time_slots; i++) {
            client->throughput_data[i] = 0;
        }

        /* modify schema name */
        strncpy(DBSchemaName, argv[ARGV_DBSCHEMANAME], W);
        if (DBSchemaName[0] == '.') {
            DBSchemaName[0] = '\0';
        }
                
        /* initialize logging */
        sprintf(errtxt_buf, "CLIENT%d", client->clientID);
        initializeLog(atoi(argv[ARGV_LOG_VERBOSITY]), errtxt_buf, 6);
        sprintf(errtxt_buf, CLIENT_LOGFILENAME_FORMAT, client->clientID);        
        err = createLog(errtxt_buf);
        if (err) {
            /* cannot continue if log file failed */
            message_client('F', NULL, "Error initializing log file..."
                           "exiting.");
            ret = E_FATAL;
            goto fin;
        }
        message_client('D', NULL, "Client thread started");

        if (uniform) {
            message_client('I', NULL, "Uniform key distribution used");
        } else {
            sprintf(errtxt_buf, "Non-uniform key distribution with A = %d "
                    "used", nurand_sid_a);
            message_client('D', NULL, errtxt_buf);
        }

        /* initialize random number generator */
#ifdef WIN32
        QueryPerformanceFrequency(&freq);
        QueryPerformanceCounter(&ticks);
        init_genrand(&client->rand, (unsigned long)(ticks.QuadPart/(freq.QuadPart/1000000)));
#else
        gettimeofday(&timev, NULL);
        init_genrand(&client->rand, (unsigned long)(timev.tv_usec));
#endif
        
        thd_mutex_lock(&g_server_count_mtx);

        err = initComm(client, errtxt_buf, client->listenPort);
        if (err) {
            ret = err;
            goto fin;            
        }
        /* Connect to the control module */
        if (err == 0) {
            sprintf(errtxt_buf, "CLIENT%d", client->clientID);
            sckControl = createConnection("127.0.0.1", connectPortControl);
            if (sckControl == 0) {
                /* cannot continue without the control module */
                message_client('F', NULL,
                               "Error in communication with control"
                               "...exiting.");
                ret = E_FATAL;
                goto fin;
            }
        }        
        
        /* Connect to the statistics module */
        sckStatistics = createConnection(argv[ARGV_STATISTICS_IP],
                                         STATISTICS_PORT);
        if (sckStatistics != 0) {
            err = initializeMessaging();
        }
        if ( (sckStatistics == 0) || (err) ) {
            /* cannot continue without the statistics module */
            message_client('F',
                           NULL,
                           "Error in communication with statistics. Exiting.");
            ret = E_FATAL;
        }
        /* Send registration to statistics */
        err = sendToStatistics(sckStatistics, MSG_REG, NULL,
            client->clientID, client->fatalCount);
        if (err) {
            message_client('F', NULL,
                           "Error in communication with statistics. "
                           "Exiting.");              
            ret = E_FATAL;
        } else {
            message_client('D', NULL, "Connecting to the statistics "
                           "module succeeded.");
        }

        thd_mutex_unlock(&g_server_count_mtx);
        if (ret) {
            goto fin;
        }       
        
        /* we have now registered to statistics and must perform
        logout (go to that label) if something fails from this on */

        if ((client->operation_mode == RUN)
            || (client->operation_mode == RUN_DEDICATED)) {
            /* Check that the transaction file exists */
            if (err == 0) {
                if (openFile(&fSQL, argv[ARGV_TRANSACTION_FILE]) ) {
                    sprintf(errtxt_buf,
                            "Transaction file '%s' not found. Exiting",
                            argv[ARGV_TRANSACTION_FILE]);
                    message_client('F', "fopen()", errtxt_buf);
                    ret = E_FATAL;
                    goto logout;
                } else {
                    sprintf(errtxt_buf,
                            "Transaction file '%s' opened.",
                            argv[ARGV_TRANSACTION_FILE]);
                    message_client('D', NULL, errtxt_buf);
                }
            }
            /* Read transaction file and create the transaction data structures */
            if (err == 0) {
                err = readSQLFile(client, fSQL);
                if (err != 0) {
                    sprintf(errtxt_buf,
                            "Error in parsing transaction file '%s'. Exiting",
                            argv[ARGV_TRANSACTION_FILE]);
                    message_client('F', "readSQLFile()", errtxt_buf);
                    ret = E_FATAL;
                    goto logout;
                }
            }       
            /* Close transaction file */
            if (fSQL != NULL) {
                if (fclose(fSQL) != 0) {
                    sprintf(errtxt_buf,
                            "Error in closing transaction file '%s'",
                            argv[ARGV_TRANSACTION_FILE]);
                    message_client('E', "fclose()", errtxt_buf);
                    err = E_ERROR;
                }
            }
        } else if (client->operation_mode == POPULATE) {
            int remainder = 0;
            subscribers_client =
                (int)((max_subs_id - min_subs_id + 1) / numberOfClientThreads);
            if (client->clientID == (firstClientID + numberOfClientThreads - 1)) {
                remainder = (max_subs_id - min_subs_id + 1) % subscribers_client;
            }
            client_min_s_id = min_subs_id +
                (client->clientID-firstClientID) * subscribers_client;
            if (remainder > 0) {
                subscribers_client += remainder;
            }
            client_max_s_id = client_min_s_id + subscribers_client -1;

            sprintf(errtxt_buf, "S_ID range: [%d, %d], "
                    "subscribers per client: %d",
                    client_min_s_id, client_max_s_id, subscribers_client);
            message_client('D', NULL, errtxt_buf);
        }
        
        if (client->operation_mode != NOP) {
            /* Connect to the target database */        
            thd_mutex_lock(&g_server_count_mtx);
            if (err == 0) {
                if (ConnectDB(&testdb_env,
                              &testdb,
                              argv[ARGV_TEST_DSN],
                              "target database") ) {
                    message_client('F', "ConnectDB()", "ConnectDB failed!");
                    err = E_ERROR;
                    ret = E_FATAL;
                } else {
                    message_client('D', NULL, "ConnectDB succeeded");
                    connection->hdbc = testdb;
                }
            }
            thd_mutex_unlock(&g_server_count_mtx);
            
            if (ret) {
                goto logout;
            }
        }

        if (client->operation_mode == RUN) {
            if (err == 0) {
                sqlerr = SQLGetInfo(testdb, SQL_CURSOR_COMMIT_BEHAVIOR,
                                    &capability, 0,0);
                if (error_c(testdb, sqlerr) != 0) {
                    message_client('E', "SQLGetInfo()", "SQLGetInfo failed");
                    err = E_ERROR;
                } else if (capability == SQL_CB_DELETE) {
                    message_client('E', "SQL_CURSOR_COMMIT_BEHAVIOR check failed",
                                   "SQL_CURSOR_COMMIT_BEHAVIOR check failed");
                    err = E_ERROR;
                }
            }        
            if (err == 0) {
                sqlerr = SQLGetInfo(testdb, SQL_CURSOR_ROLLBACK_BEHAVIOR,
                                    &capability, 0,0);
                if (error_c(testdb, sqlerr) != 0) {
                    message_client('E', "SQLGetInfo()", "SQLGetInfo failed");
                    err = E_ERROR;
                } else if (capability == SQL_CB_DELETE) {
                    message_client('E', "SQL_CURSOR_ROLLBACK_BEHAVIOR "
                                   "check failed",
                                   "SQL_CURSOR_ROLLBACK_BEHAVIOR check failed");
                    err = E_ERROR;
                }
            }
        }

        /* Test some target database functionality and run the tests */
        /* break the while loop if something fails before starting the test */
        while (err == 0) {
            
            if (client->operation_mode != NOP) {
                /* Check cursor commit/rollback capability */            
                /* Turn off the autocommit mode */
                sqlerr = SQLSetConnectAttr(testdb, SQL_ATTR_AUTOCOMMIT,
                                           (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);
                if (error_c(testdb, sqlerr)) {
                    message_client('E', "SQLSetConnectAttr()",
                                   "SQLSetConnectAttr failed");
                }
                
                /* run per-connection init script */
                if (init_sql_file[0] != '\0') {
                    err = processSQLFile(init_sql_file, &testdb, &g_server, NULL);
                }
                /* if the file did not exist, ignore the error and continue */
                
                err = setupTargetDB(&testdb, connection, client);
                if (err != 0) {
                    message_client('E',
                                   NULL,
                                   "Could not setup target DB ...exiting");
                    ret = E_FATAL;
                    break;
                }
            }
            
            /* we are ready to start the test, send OK message to control */
            err = sendDataS(sckControl, client->clientID, MSG_OK, NULL);
            if (err != 0) {
                message_client('E',
                               NULL,
                               "Sending OK message to control failed.");
                err = E_FATAL;
                break;
            }
            
            /* wait for TIME message */
            err = waitForControlMessage(client, sckControl,
					&last_control_message);
            if (err) break;
            
            /* wait for START TEST message */
            if (last_control_message != MSG_INTR) {
                err = waitForControlMessage(client, sckControl,
                                            &last_control_message);
                if (err) break;
            }
            else {
                /* We hit an error (INTR message received before
                   TIME message */
                err = 1;
            }
            
            if (last_control_message == MSG_STARTTEST) {
                /* now go on with the test */
                switch (client->operation_mode) {
                    case RUN:
                    case RUN_DEDICATED:
                            /* run the benchmark */
                        err = runTests(client,
                                       &argv[CLIENT_STATIC_ARGC],
                                       (argc-CLIENT_STATIC_ARGC)/2,
                                       atoi(argv[ARGV_RAMPUP_TIME]),
                                       atoi(argv[ARGV_TEST_TIME]),
                                       DBSchemaName,
                                       client->operation_mode);
                        break;
                    case POPULATE:
                        err = populate(init_sql_file,
                                       connection->hdbc,
                                       DBSchemaName,
                                       popl_size,
                                       commitblock_size,
                                       serial_keys,
                                       client_min_s_id,
                                       client_max_s_id);
                        break;
                    default:
                        /* POPULATE_CONDITIONALLY is converted to
                           POPULATE or NOP */
                        break;
                }
                
                if (err) {
                    message_client('E', "runTests()",
                                   "Error in running the client...exiting");
                }

                if (client->operation_mode != NOP) {
                    /* Turn on autocommit-mode */
                    sqlerr = SQLSetConnectAttr(testdb, SQL_ATTR_AUTOCOMMIT,
                                               (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
                    if (error_c(testdb, sqlerr)) {
                        message_client('E', "SQLSetConnectAttr()",
                                       "SQLSetConnectAttr failed");
                    }
                }
            }
            break;
        } /* end while */
        
        /* Cleanup phase */
        
        /* Disconnect from the target database */
        if (testdb != SQL_NULL_HDBC) {
            if (DisconnectDB(&testdb_env, &testdb, "target database")) {
                message_client('E', "client_thread()", "DisconnectDB failed!");
                err = E_ERROR;
            }
            else {
                message_client('D', NULL, "DisconnectDB succeeded");
            }
        }        
                
        /* send results to statistics */
        if (last_control_message == MSG_STARTTEST) {
            if (client->operation_mode == RUN || client->operation_mode == RUN_DEDICATED) {
                if (err == 0) {
                    err = sendResults(client, sckStatistics);
                    if (err != 0) {
                        message_client('E',
                                       NULL,
                                       "Sending results to statistics failed");
                    }
                } else {
                    message_client('W',
                                   NULL,
                                   "Did not send results to statistics because of "
                                   "error(s)");
                }
            }
            
        }
        
        {
            int server_count;
            
            thd_mutex_lock(&g_server_count_mtx);
            server_count = --g_server_count;
            thd_mutex_unlock(&g_server_count_mtx);
            
            if (server_count == 0) {
#ifdef ACCELERATOR
#ifdef CACHE_MODE
                /* the frontend is now running, wait for connector
                   to propagate everything */
                writeLog('I',
                         "Press enter when you are ready to stop "
                         "the benchmark.");
                c = getchar();
#endif
#endif
                /* stop local server */
                err = stopServer(g_server);
                if (err != 0) {
                    char msg[W_L];
                    sprintf(msg, "Could not stop database server (%s), "
                            "error %d",
                            server_name,
                            err);
                    message_client('E', NULL, msg);
                }
            }
        }
logout:
        /* Log out from statistics */
        err = sendToStatistics(sckStatistics, MSG_LOGOUT, NULL,
            client->clientID, client->fatalCount);
        
        /* free results memory */
        free(client->throughput_data);

        /* free transaction memory structures */
        while (client->tr_head != NULL) {
            trans_t* tr = client->tr_head->trans_next;
            while (client->tr_head->trans_sql != NULL) {
                sql_t* s = client->tr_head->trans_sql->sql_next;
                while (client->tr_head->trans_sql->sql_func_list != NULL) {
                    func_t* fc
                        = client->tr_head->trans_sql->sql_func_list->func_next;
                    free(client->tr_head->trans_sql->sql_func_list->func_var_value);
                    free(client->tr_head->trans_sql->sql_func_list);
                    client->tr_head->trans_sql->sql_func_list = fc;
                }
                free(client->tr_head->trans_sql->sql_clause);
                free(client->tr_head->trans_sql);
                client->tr_head->trans_sql = s;
            }
            while (client->tr_head->trans_errors != NULL) {
                sqlerror_t* e = client->tr_head->trans_errors->error_next;
                free(client->tr_head->trans_errors);
                client->tr_head->trans_errors = e;
            }
            free(client->tr_head);
            client->tr_head = tr;
        }
        while (client->variables != NULL) {
            var_t* va = client->variables->var_next;
            free(client->variables);
            client->variables = va;
        }
            
#ifdef _DEBUG
        /* For TATP software performance analysis
        -> not needed in actual benchmark runs */
        sprintf(errtxt_buf, "CLIENT_TIMINGS_%d.CSV", client->clientID);
        saveMyTimings(errtxt_buf);
#endif
fin:
        /* clean up sockets etc. */
        message_client('D', NULL, "Cleaning up client TCP connections");

        if (sckStatistics) {
            /* close the statistics socket */
            err = disconnectConnection(&client->comm, sckStatistics);
            if (err != 0) {
                message_client('W', "finalize()",
                               "disconnectCommunication() [statistics] failed.");
           } 
        }

        if (sckControl) {
            /* close the control socket */
            err = disconnectConnection(&client->comm, sckControl);
            if (err != 0) {
                message_client('W', "finalize()",
                               "disconnectCommunication() [control] failed.");
            }
        }
        
        /* close the listener socket */
        err = finalizeCommunication(&client->comm);
        if (err != 0) {
            message_client('W', "finalize()",
                "finalizeCommunication() [listener] failed.");
        }

        message_client('D', NULL, "Finished client thread execution");

        /* close the log file */
        finalizeLog();
        
        /* set exit value */
        if (ret) {
            return ret;
        } else {            
            return 0;
        }
}

/*##**********************************************************************\
 *
 *      setupTargetDB
 *
 * DBMS-specific initializations.
 *
 * Parameters:
 *      testdb 
 *          the target database
 *
 *      connection 
 *          pointer to connection struct
 *
 *      client 
 *          pointer to struct holding client data
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
RETCODE setupTargetDB (SQLHDBC* testdb, connection_t* connection,
                       client_t* client) {
        RETCODE sqlerr;
        SQLHSTMT hstmt;
        char version[W] = {'\0'};
        
#ifdef TC_COUNT
        char    errtxt_buf[256];
	    SQLINTEGER tf_level = 0;
        SQLINTEGER tc_level = 0;
#ifdef _DEBUG
        char    debug[256];
#endif
#endif
        sqlerr = detectTargetDB (testdb, &connection->db, version, 0);
        
        if (sqlerr != SQL_SUCCESS) {
            return sqlerr;
        }
        
        if (connection->db == DB_INFORMIX) {
            sqlerr = SQLAllocHandle(SQL_HANDLE_STMT, *testdb, &hstmt);
            if (sqlerr == SQL_SUCCESS) {
                sqlerr = SQLExecDirect(hstmt,
                                       (SQLCHAR*) "SET LOCK MODE TO WAIT",
                                       SQL_NTS);
                if (error_c(*testdb, sqlerr) != 0) {
                    message('F', "SQLExecDirect failed. 'SET LOCK MODE' "
                            "failed.");
                } else {
                    message('I', "SQLExecDirect succeeded. 'SET LOCK MODE' "
                            "succeeded.");
                }
                SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
            }
            if (sqlerr != SQL_SUCCESS) {
                return sqlerr;
            }
	}
        
#ifdef TC_COUNT
        
        client->solid_ha_stat = 0;
        
        if (connection->db == DB_SOLID) {
            strcpy(connection->solid_connection_one, "");
            strcpy(connection->solid_connection_two, "");
            
			/* failure transparency level */   
			sqlerr = SQLGetConnectAttr(
                    *testdb,
                    SQL_ATTR_TF_LEVEL,
                    &tf_level, 
					0,
                    NULL);
            
            checkError(sqlerr, SQL_HANDLE_DBC, *testdb, NULL, NULL, NULL);
            
			/* Preferred Access level == load balancing status */
			sqlerr = SQLGetConnectAttr(
                    *testdb,
                    SQL_ATTR_PA_LEVEL,
                    &tc_level, 
					0,
                    NULL);
            checkError(sqlerr, SQL_HANDLE_DBC, *testdb, NULL, NULL, NULL);
            
			/* primary server */
			sqlerr = SQLGetConnectAttr(
                    *testdb,
                    SQL_ATTR_TC_PRIMARY,
                    connection->solid_connection_one,
                    0,
                    NULL);
            checkError(sqlerr, SQL_HANDLE_DBC, *testdb, NULL, NULL, NULL);

			/* secondary server */
			sqlerr = SQLGetConnectAttr(
                    *testdb,
                    SQL_ATTR_TC_SECONDARY,
                    connection->solid_connection_two,
                    0,
                    NULL);
            
            checkError(sqlerr, SQL_HANDLE_DBC, *testdb, NULL, NULL, NULL);

            trim_connect_string(connection->solid_connection_one);
            trim_connect_string(connection->solid_connection_two);
#ifdef _DEBUG
            if (strlen(connection->solid_connection_one) == 0) {
                message_client('I', NULL, "No TC ODBC attributes available");
            }
			message_client('I', NULL, connection->solid_connection_one);
            message_client('I', NULL, connection->solid_connection_two);
            
            sprintf(debug, "tf_level: %d tc_level: %d", (int)tf_level, (int)tc_level);
            message_client('I', NULL, debug);
#endif
            if (tf_level != 0) {
                message_client('I', NULL, "Transparent failover detected.");
            }
            
            if (tc_level == 1) {
                client->solid_ha_stat = 1; /* turns on two-column TPS table
                                              processing */
                message_client('I', NULL, "Load balancing detected.");
                if (strlen(connection->solid_connection_two) == 0) {
                    sprintf(errtxt_buf, "Assigned workload server: %s",
                            connection->solid_connection_one);
                } else {
                    sprintf(errtxt_buf, "Assigned workload server: %s",
                            connection->solid_connection_two);
                }
                message_client('I', NULL, errtxt_buf);
            }
        }
#endif        
        return 0;
}

/*##**********************************************************************\
 *
 *      sendResults
 *
 * Sends benchmark results to given socket.
 *
 * Parameters:
 *      client
 *          pointer to client strcut
 *
 *      sck 
 *          socket to use
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int sendResults (client_t* client, SOCKET sck)
{
        struct message_dataS mdata;   /* Message struct */
        trans_t *transaction;
        int err = 0;
        int i = 0;
        int j = 0;
        int argc = g_argc;
        char** argv = g_argv;
        char msg[512];
                            
        while ((i < num_of_time_slots) && (err == 0)) {
            if (client->throughput_data[i] > 0) {
                mdata.utime = time(NULL);
                mdata.sdata.mqth.timeSlotNum = i;
                mdata.sdata.mqth.transCount = client->throughput_data[i];
                err = sendToStatistics(sck, MSG_MQTH, &mdata,
                                       client->clientID, client->fatalCount);
            }
            i++;
        }
        
        /* send response times */
        for (transaction = client->tr_head; transaction != NULL;
             transaction = transaction->trans_next) {
            mdata.sdata.mqth.timeSlotNum = 0;
            mdata.sdata.mqth.transCount = 0;
            
            mdata.utime = time(NULL);

            for (j = CLIENT_STATIC_ARGC; j < argc; j = j+2) {
                if (strcmp(argv[j], transaction->trans_name) == 0) {
                    /* transaction found in arguments */
                    break;
                }
            }
            if (j == argc) {
                /* transaction was not run, jump to next transaction */
                continue;
            }
                
            for (i = 0; i < MAX_RESP_TIME_SLOTS; i++) {
#ifndef LINEAR_RESPONSE_SCALE
                if (i < response_time_bound_count) {
                    
/*#ifdef _DEBUG*/
                    sprintf(msg,
                            "Txn %s slot %d, bdry %d, hits %d",
                            transaction->trans_name, i,
                            response_time_bounds[i],
                            transaction->response_times[i]);
                    message_client('D', "sendResults()", msg);
/*#endif*/
                    
                    mdata.sdata.resptime.slot = i;
                    mdata.sdata.resptime.responseTimeBound =
                        response_time_bounds[i];
#else
                if (transaction->response_times[i] > 0) {
                    mdata.sdata.resptime.responseTime = i;
#endif
                    mdata.sdata.resptime.transactionCount =
                        transaction->response_times[i];
                    strncpy(mdata.sdata.resptime.transactionType,
                            transaction->trans_name,
                            TRANSACTIONTYPE_SIZE);                    
                    
                    err = sendToStatistics(sck, MSG_RESPTIME, &mdata,
                                           client->clientID,
                                           client->fatalCount);
                }
            }
        }
            
        return err;
}

/*##********************************************************************** \
 *
 *      sendToStatistics
 *
 * Sends a message to the statistics process.
 *
 * Parameters:
 *      sck
 *          socket in which to send
 *
 *      messageType
 *          message type code
 *
 *      data
 *          message data struct
 *
 *      clientID
 *          ID of the sender
 *
 *      fatalCount
 *          amount of fatal errors (used in LOGOUT message)
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int sendToStatistics (SOCKET sck, int messageType, struct message_dataS *data,
                      int clientID, int fatalCount)
{
	int err = 0;
	struct message_dataS *message_data;  /* pointer */
	struct message_dataS t_data; 	     /* temporary struct to hold
                                            the data to be sent */
    
	if (data != NULL) {
		/* use data struct given as a parameter */
		message_data = data;
	} else {
		/* generate message data in this function */
		/* common elements */
		t_data.utime = time(NULL);
		t_data.sdata.reg.testID = testID;
		message_data = &t_data;
	}

	if (sck != 0) {
		switch (messageType) {
		case (MSG_REG) :
			message_data->sdata.reg.data = 0;
			err = sendDataS(sck, clientID, MSG_REG, message_data);
			if (err != 0) {
				message_client('E', NULL,
					"Sending registration to statistics failed.");
			} else {
				message_client('D', NULL,
					"Sending registration to statistics succeeded.");
			}
			break;
		case (MSG_LOGOUT) :
			message_data->sdata.reg.data = fatalCount;
			err = sendDataS(sck, clientID, MSG_LOGOUT, message_data);
			if (err != 0) {
				message_client('E', NULL,
					"Sending logout to statistics failed.");
			}
			else {
				message_client('D', NULL,
					"Sending logout to statistics succeeded.");
			}
			break;
		case (MSG_MQTH) :
			err = sendDataS(sck, clientID, MSG_MQTH, message_data);
			if (err != 0) {
				message_client('E', NULL,
					"Sending MQTH value to statistics failed.");
			}
			break;
		case (MSG_RESPTIME) :
 		        err = sendDataS(sck, clientID, MSG_RESPTIME,
					message_data);
			if (err != 0) {
				message_client('E', NULL,
					"Sending response time value to statistics failed.");
			}
			break;
		default :
			break;
		}
	}
	return err;
}

/*##**********************************************************************\
 *
 *      waitForControlMessage
 *
 * Waits for the specified message and responses to
 * it depending of the received message type
 *
 * Parameters:
 *      client 
 *          pointer to client struct
 *
 *      sck 
 *          socket to use for communication
 *
 *      previous_control_message
 *          pointer to use for reading/writing latest message info
 *          
 * Return value:
 *      0  - success
 *     !0  - error
 */
int waitForControlMessage (client_t* client, SOCKET sck,
                           int *previous_control_message)
{
        int senderID = 0;              /* Sender ID of the received message */
        int messageType = 0;           /* Type of the received message. */
        struct message_dataS data;     /* Struct to hold the received data */
        
        int err = 0;
        char msg[256];

        /* Wait for the message */
        /* blocking */
#ifndef OLD_FD
        err = receiveDataS_mutexed(&client->comm, &senderID, &messageType, &data, &comm_mutex);
#else        
        err = receiveDataS(&client->comm, &senderID, &messageType, &data);
#endif
        if (err != 0) {
            message_client('E', "waitForMessage()", "Error receiving "
                           "message...");
            return E_FATAL;
        }
#ifdef _DEBUG
        sprintf(msg,
                "Received message from sender ID:%d: message type %d",
                senderID, messageType);
        message_client('D', "receiveDataS()", msg);
#endif        
        
        /* check sender */
        /* NOTE: only main and remote control are allowed to communicate
           with clients */
        if (!(senderID <= MAIN_CONTROL_ID)) { /* this includes both */
            sprintf(msg,
                    "Received a message from an unexpected sender id '%d'",
                    senderID);
            message_client('E', "receiveDataS()", msg);
            return E_FATAL;
        }
        
        /* we got what was expected, send response message */
        switch (messageType) {
            case (MSG_TIME) :
                /* check message type */
                if (*previous_control_message != MSG_OK) {
                    sprintf(msg,
                            "Unexpected message sequence: got "
                            "type %d",messageType);
                    message_client('E', "receiveDataS()", msg);
                    return E_FATAL;
                }
                
                /* the test has been running x milliseconds so far */
                client->testTimeOffset = data.sdata.reg.data;
                
                /* initialize the clock in client side */
                
                /* Start measuring test time */
                startTimer(&client->global_timer);
                
                /* send the received time back to control */
                /* we send the whole data unchanged */
                err = sendDataS(sck, client->clientID, MSG_TIME, &data);
                if (err != 0) {
                    message_client('E', NULL, "Sending TIME "
                                   "message to control failed.");
                    return E_FATAL;
                }
                break;
            case (MSG_STARTTEST) :
                if (*previous_control_message != MSG_TIME) {
                    sprintf(msg,
                            "Unexpected message sequence: got "
                            "type %d", messageType);
                    message_client('E', "receiveDataS()", msg);
                    return E_FATAL;
                }
                break;
                
            case (MSG_INTR) :
                /*
                  if (*previous_control_message != MSG_TIME) {
                  sprintf(msg,
                  "Unexpected message sequence: got \
                  type %d", messageType);
                  message_client('E', "receiveDataS()", msg);
                  return E_FATAL;
                  }
                */
                
                /* interrupt the test */
                break;
                
            default :
                /* unspecified, error */
                sprintf(msg,
                        "Unexpected message: got message type %d",
                        messageType);
                message_client('E', "receiveDataS()", msg);
                return E_FATAL;
                
                /* do nothing but continue */
                break;
        }
        /* update state */
        *previous_control_message = messageType;
        return 0;
}

/*##**********************************************************************\
 *
 *      initComm
 *
 * Initializes the communication (socket) system for the client
 *
 * Parameters:
 *      client 
 *          pointer to client struct
 *
 *      clientName 
 *          name of the client
 *
 *      port 
 *          listening port for the client
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int initComm(client_t* client, char *clientName, int port)
{
        int err;

        /* Initialize the communication system */
        err = initializeCommunication(&client->comm, clientName);
        if (err) {
            writeLog('F', "Cannot initialize the communication system");
            return E_FATAL;
        }
        /* Create the socket listener for Control */
        err = createListener(&client->comm, (unsigned short)port);
        if (err) {
            writeLog('F', "Cannot create the socket listener");
            return E_FATAL;
        }
        /* Initialize the messaging system */
        err = initializeMessaging();
        if (err) {
            writeLog('F', "Cannot initialize the messaging system");
            return E_FATAL;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      checkAndUpdateMQTHSlot
 *
 * Check and change if needed the MQTH time slot.
 * The MQTh resolution defines the size of the MQTh time
 * slot. If the resolution is 1 second then the MQTh
 * time slot length is 1 second, if the resolution is 10 secs
 * the the MQTh time slot length is 10 secs, and so on.
 *
 * This method is called every time a transaction is run
 * to check whether there is a need to change the MQTh time slot
 * (indicated by the variable current_mqth_time_slot. The variable gives
 * the ordinal
 * number of the time slot starting from the beginning of the
 * benchmark).
 *
 * Parameters:
 *      client 
 *          pointer to client struct
 *
 * Return value:
 *      0  - always
 */
int checkAndUpdateMQTHSlot(client_t* client)
{
        __int64 current_test_time;
        
        /* read the timer (to be able to check when to stop the test) */
        readTimer(&client->global_timer, &current_test_time);
        
        client->current_mqth_time_slot =
            (current_test_time +
             client->testTimeOffset) / (throughput_resolution*1000);
        return 0;
}

/*##**********************************************************************\
 *
 *      readSQLFile
 *
 * A simple parser that reads a file that contains the SQL
 * transactions (consisting of one or more sql clauses) and
 * creates the needed data structures.
 *
 * Parameters:
 *      client
 *          pointer to client struct
 *
 *      fSQL
 *          transaction file handle
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int readSQLFile(client_t* client, FILE *fSQL)
{
        /* The initial parser mode */
        enum mode_e mode = SEEK_FOR_TRANSACTION;
        int firstline = 0;
        int sql_clause_amount = 0;
        int length = 0;
        int i, tr_name_len;
        int err_len = 0;
        int err = 0;
        int sqlclause_open = 0;
        int sqlclause_buf_len = 0;
        int single_closing_bracket = 0;
        char *c;
        char line_readbuffer[READ_BUFFER_SIZE];
        char *collected_string = NULL;
        char *sqlclause = NULL;
        char *errtemp = NULL;
        char *temp;
        char errorcode[ERROR_CODE_LENGTH];
        char errtxt_buf[256];
        regex_t *r;
        sqlerror_t *error;
        trans_t *t = NULL;
        sql_t *s, *prev_s;
        char *saveptr1;
        
        /* initialize regexp parser */
        r = (regex_t *) malloc(sizeof(regex_t));
        if (r == NULL) {
            message_client('F', "readSQLFile()",
                           "Cannot reserve memory for regex_t.");
            return E_FATAL;
        }
        
        firstline = 1;
        /* read the transaction file line-by-line */
        while (readFileLine(fSQL, line_readbuffer,
                            READ_BUFFER_SIZE) != -1) {
            if (firstline) {
                /* check that the file identification tag exists in
                   the beginning of the file */
                firstline = 0;
                if (strncmp(line_readbuffer, "//tatp_transaction", 17)
                    != 0) {
                    message_client('F', "readSQLFile()",
                                   "The transaction file has "
                                   "wrong or no identification line");
                    free(r);
                    return E_FATAL;
                }
                continue;
            }
            
            /* clean up the line */
            removeComment(line_readbuffer);
            length = removeExtraWhitespace(line_readbuffer,
                                           READ_BUFFER_SIZE);
            trim(line_readbuffer);
            
            /* Empty line -> next iteration round */
            if ((length == 0) ||
                ((length == 1) && (line_readbuffer[0] == ' ')))
                continue;
            
            
            if (single_closing_bracket &&
                ((c = strchr(line_readbuffer, '{')) != NULL)) {
				free(collected_string);
				collected_string = NULL;
				mode = SEEK_FOR_TRANSACTION;
			}
            
			/* copy data from readbuffer to collected_string
			   variable (that may already contain data from
			   previously read lines) */
			if (collected_string == NULL) {
				collected_string =
                    (char*)malloc((strlen(line_readbuffer) + 1) *
                                  sizeof(char));
				strncpy(collected_string, line_readbuffer,
                        strlen(line_readbuffer) + 1);
			}
			else {
				collected_string =
                    (char*)realloc(collected_string,
                                   strlen(collected_string) + 1
                                   + strlen(line_readbuffer) + 1);
				strcat(collected_string, " ");
				strncat(collected_string, line_readbuffer,
                        strlen(line_readbuffer));
			}
            
			/* match transaction start */
			if ((c = strchr(collected_string, '{')) != NULL) {
				if (mode != SEEK_FOR_TRANSACTION) {
					/* previous transaction not ended with '}'*/
					sprintf(errtxt_buf, "Unexpected '{' found while "
                            "parsing: '%s'",
                            collected_string);
					message_client('F', "readSQLFile()", errtxt_buf);
					free(collected_string);
					free(r);
					return E_FATAL;
				}
				else {
					/* a transaction begins */
					if (!(fullMatch(r, collected_string,
                                    "^ *[a-zA-Z]([a-zA-Z0-9_\\-])* *\\{ *$"))) {
						/* the line passed the regexp check */
						sql_clause_amount = 0;
						single_closing_bracket = 0;

						/* create new entry in transaction list */
						t = (trans_t*)malloc(sizeof(trans_t));
						if (t == NULL) {
							/* Dynamic memory allocation failed */
							message_client('F', "readSQLFile()",
								"Dynamic memory allocation failed");
							free(r);
							return E_FATAL;
						}
						/* Collect the transaction name first */
						tr_name_len = c - collected_string;
						strncpy(t->trans_name, collected_string, tr_name_len);
						t->trans_name[tr_name_len] = '\0';
						trim(t->trans_name);
						/* Initialize the rest of the transaction structure */
						t->trans_next = NULL;
						t->trans_sql = NULL;
						t->trans_errors = NULL;
						t->commitCount = 0;
						t->rollbackCount = 0;
						t->ignoredCount = 0;
#ifdef LATENCYBOUNDS
                        t->maxLatency = 0;
                        t->minLatency = INT_MAX;
#endif                        
						/* reserve space for response time data */
						for (i = 0; i < MAX_RESP_TIME_SLOTS; i++) {
							t->response_times[i] = 0;
						}

						/* Change the parser mode */
						mode = IN_TRANSACTION;

						/* add to the beginning of the linked list */
						if (client->tr_head != NULL) {
							t->trans_next = client->tr_head;
						}
						client->tr_head = t;

						/* prepare to collect the string containing
                           a sql clause */
						free(collected_string);
						collected_string = NULL;
					}
					else {
						/* transaction name contains other than
                           a-z, A-Z, 0-9, '-' or '_' */
						sprintf(errtxt_buf,
                                "Transaction name '%s' contains unaccepted "
                                "characters.",
                                collected_string);
						message_client('F', "readSQLFile()", errtxt_buf);
						free(collected_string);
						free(r);
						return E_FATAL;
					}
				}
				/* match transaction end */
			}
			else if ((c = strchr(collected_string, '}')) != NULL) {

				if (mode != IN_TRANSACTION) {
					/* transaction start tag '{' was not found */
					sprintf(errtxt_buf,
                            "Unexpected '}' found while parsing '%s'",
                            collected_string);
					message_client('F', "readSQLFile()", errtxt_buf);
					free(collected_string);
					free(r);
					return E_FATAL;
				}
				else {
					/* end of transaction */
					if (sqlclause_open) {
						/* sql clause list still 'open' */
						sprintf(errtxt_buf,
                                "Syntax error: ';' missing from the end "
                                "of the sql clause: '%s'",
                                collected_string);
						message_client('F', "readSQLFile()", errtxt_buf);
						free(collected_string);
						free(sqlclause);
						free(r);
						return E_FATAL;
					}

					/* the last line should contain single '}' or well-formed
                       ERRORS ALLOWED string */
					if (!(fullMatch(r, collected_string,
                                    "^ *}( *\\(ERRORS ALLOWED ([A-Za-z0-9])*(, "
                                    "*([A-Za-z0-9])*)*\\))?$"))) {
						if ((strlen(collected_string) == 1)
                            && (collected_string[0] == '}')) {
							single_closing_bracket = 1;
						}
						else {
							single_closing_bracket = 0;
						}
						if (!(single_closing_bracket)) {
							temp = NULL;
							i = 0;
							temp = strchr(collected_string, 'D')+1;
							if (temp != NULL) {
								/* locate error code */
#ifdef WIN32
								c = strtok (temp," ,)");
#else
								c = strtok_r (temp," ,)", &saveptr1);
#endif
								/* build allowed errors list */
								while (c != NULL) {
									err_len = strlen(c)+1;
									errtemp = (char*)calloc(err_len,
                                                            sizeof(char));
									if (errtemp == NULL) {
										message_client('F', "readSQLFile()",
                                                       "Dynamic memory "
                                                       "allocation failed");
										free(r);
										return E_FATAL;
									}
									strncpy(errtemp, c, err_len);

									if (strcmp(errtemp, "") != 0) {
										sscanf(errtemp, "%s", errorcode);
										error = (sqlerror_t*)malloc(
                                                sizeof(sqlerror_t));
										if (error == NULL) {
											message_client('F', "readSQLFile()",
                                                           "Dynamic memory "
                                                           "allocation failed");
											free(r);
											free(errtemp);
											return E_FATAL;
										}
										strncpy(error->error_code, errorcode,
                                                ERROR_CODE_LENGTH);
										error->error_next = NULL;
#ifdef _DEBUG
										error->error_amount = 0;
#endif
										if (client->tr_head->trans_errors
                                            != NULL) {
											error->error_next = client->
                                                tr_head->trans_errors;
										}
										else {
											error->error_next = NULL;
										}
										client->tr_head->trans_errors = error;
									}
#ifdef WIN32
								c = strtok (NULL, " ,)");
#else
								c = strtok_r (NULL," ,)", &saveptr1);
#endif
									free(errtemp);
									errtemp = NULL;
								}
							}
							free(collected_string);
							collected_string = NULL;
						}
					}
					else {
						/* "ERRORS ALLOWED ..." line contained syntax error */
						sprintf(errtxt_buf, "Syntax error in: '%s'",
                                collected_string);
						message_client('F', "readSQLFile()", errtxt_buf);
						free(collected_string);
						free(r);
						return E_FATAL;
					}
					if (!(single_closing_bracket)) {
						/* transaction block ended */
						if (sql_clause_amount == 0) {
							message_client('F',
                                           "readSQLFile()",
                                           "No SQL clauses found "
                                           "inside a transaction.");
							free(collected_string);
							free(r);
							return E_FATAL;
						}
						mode = SEEK_FOR_TRANSACTION;
					}
				}
			}
			else if (mode == IN_TRANSACTION) {
				sqlclause_open = 1;
				/* if the line terminates a sql clause,
                   save the clause into list */
				if (collected_string[strlen(collected_string)-1] == ';') {
					/* SQL clause ended */
					sql_clause_amount++;
					s = t->trans_sql;
					prev_s = s;
					while (s != NULL) {
						prev_s = s;
						s = s->sql_next;
					}
					if (prev_s == NULL) {
						t->trans_sql = (sql_t*)calloc(1, sizeof(sql_t));
						if (t->trans_sql == NULL) {
							message_client('F', "readSQLFile()",
                                           "Dynamic memory allocation failed");
							free(errtemp);
							free(r);
							return E_FATAL;
						}

						s = t->trans_sql;
					}
					else {
						prev_s->sql_next = (sql_t*)calloc(1, sizeof(sql_t));
						if (prev_s->sql_next == NULL) {
							message_client('F', "readSQLFile()",
                                           "Dynamic memory allocation failed");
							free(errtemp);
							free(r);
							return E_FATAL;
						}

						s = prev_s->sql_next;
					}

					/* generate sqlrecord structure */
					err = buildSQLRecord(collected_string, &sqlclause, s);
					if (err != 0) {
						/* something wrong */
						free(errtemp);
						free(r);
						return err;
					}

					free(collected_string);
					collected_string = NULL;

					/* connect the sql clause list to the sql struct */
					sqlclause_buf_len = strlen(sqlclause);
					s->sql_clause = (char*)calloc(sqlclause_buf_len,
                                                  sizeof(char));
					if (s->sql_clause == NULL) {
						message_client('F', "readSQLFile()",
                                       "Dynamic memory allocation failed");
						free(errtemp);
						free(r);
						return E_FATAL;
					}
					/* remove ';' from the end of the clause */
					strncpy(s->sql_clause, sqlclause, sqlclause_buf_len-1);
					s->sql_clause[sqlclause_buf_len-1] = '\0';

					s->sql_statement = SQL_NULL_HSTMT;

					/* reset variables for the next sql clause */
					free(sqlclause);
					sqlclause = NULL;
					s->sql_next = NULL;
					sqlclause_open = 0;
				}
			}
        }
        free(collected_string);
        free(errtemp);
        free(r);
        return 0;
}

/*#***********************************************************************\
 *
 *      gen_subscriber_id
 *
 * Generate a subscriber ID using either uniform or
 * nonuniform value distribution.
 *
 * Parameters:
 *      client 
 *          pointer to client struct
 *
 * Return value:
 *      subscriber ID
 */
static long gen_subscriber_id(client_t* client)
{
        long i;
#ifdef _DEBUG
        char errtxt_buf[256];
#endif
        if (nurand_sid_a != 0) {
                i = get_nurand(&client->rand, nurand_sid_a,
                               min_subs_id, max_subs_id);
        } else {
                i = get_random(&client->rand,
                               min_subs_id, max_subs_id);
        }
        
#ifdef _DEBUG
        sprintf(errtxt_buf, "Subscriber ID = %ld", i);
        message_client('X', "gen_subscriber_id()", errtxt_buf);
#endif
        
        return i;
}

/*#***********************************************************************\
 *
 *      rnd_client
 *
 * Returns a random number. Value scale is controlled with
 * the parameter 'param'. The difference from 'rnd' function
 * is that rnd_client is able to generate NU distribution keys.
 *
 * Parameters:
 *      client
 *          pointer to client struct
 *
 *      param
 *          string describing the random number type
 *
 *      string
 *          dummy, should be NULL
 *
 * Return value:
 *          long value containing the random number
 */
static long rnd_client(client_t* client, const char* param, char* string)
{
        unsigned long i;
        if (strncmp(param, "s_id", 4) == 0) {
            i = gen_subscriber_id(client);
        }
        else {
            i = rnd(&client->rand, param, string);
        }
        return i;
}

/*#***********************************************************************\
 *
 *      rndstr_client
 *
 * Returns a random string. Value scale is controlled with
 * the parameter 'param'. The difference from 'rnd' function
 * is that rnd_client is able to generate NU distribution keys.
 *
 * Parameters:
 *      client 
 *          pointer to client struct
 *
 *      param
 *          string describing the random string type
 *
 *      string
 *           pointer to string that the random string should
 *			 be put in
 *
 * Return value:
 *       0  - success
 *      !0  - error
 */
static long rndstr_client(client_t* client, const char *param, char *string)
{
        int i;
        if (strncmp(param, "sub_nbr",7) == 0
            || strncmp(param, "numberx", 7) == 0) {            
            i = sub_nbr_gen(gen_subscriber_id(client), string);
        } else {
            i = rndstr(&client->rand, param, string);
        }
        return i;
}

/*##**********************************************************************\
 *
 *      buildSQLRecord
 *
 * Extracts the functions etc. from a SQL clause which was
 * read from a file and returns the SQL clause suitable for
 * use (with ?-placeholders)
 *
 * Parameters:
 *      clause
 *          raw SQL clause
 *
 *      newclause 
 *			pointer to pointer to string that will store the
 *		    modified SQL clause
 *
 *      s 
 *          pointer to sql struct
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int buildSQLRecord(char* clause, char** newclause, sql_t* s)
{
        int pos;
        char *modifiedclause = NULL;
        int offset = 0;
        char *c;
        char *temp = NULL;
        int pos2;
        int paramcount;
        char fu[NAME_LENGTH];
        char paramtype[NAME_LENGTH];
        char variablename[NAME_LENGTH];
        char field_name[NAME_LENGTH];
        char errtxt_buf[256];
        char format[24];
        func_t *f;
        func_t *prev_f;
        regex_t *r;
        
        /* initialize the regexp parser */
        r = (regex_t *) malloc(sizeof(regex_t));
        if (r == NULL) {
            message_client('F', "buildSQLRecord()",
                           "Cannot reserve memory for regex_t.");
            return E_FATAL;
        }

        s->sql_func_list = NULL;
        modifiedclause = (char*)calloc(strlen(clause)+1, sizeof(char));

        /* replace everything that is between < > tags */
        while ((pos = strcspn(clause+offset, "<")) !=
               strlen(clause+offset)) {
            if (clause[offset+pos-1] == '\\') {
                /* normal less-than < */
                strncat(modifiedclause, clause+offset, pos-1);
                strncat(modifiedclause, clause+offset+pos,1);
                offset = offset + pos + 1;
            }
            else {
                /* < tag start */
                strncat(modifiedclause, clause+offset, pos);

                pos2 = strcspn(clause+offset, ">");
                if (((c = strchr(clause+offset, '>')) == NULL) ||
					(pos2<pos) || (clause[offset+pos2-1] == '\\')) {
                    message_client('F', "buildSQLRecord()",
                                   "Syntax error: end tag '>' not found.");
                    free(r);
                    free(modifiedclause);
                    return E_FATAL;
                }

                temp = (char*)calloc(pos2-pos, sizeof(char));
                strncpy(temp, clause+offset+pos+1, pos2-pos-1);
                temp[pos2-pos-1] = '\0';

                /* check that the tag contains a valid function
                   and parameters */
                /* type (first field in tag) is not checked */
                if ((!(fullMatch(r, temp, "^[^ <]+ (rnd|rndstr)( [^ ]+)?$")))
					| (!(fullMatch(r, temp, "^[^ <]+ value [^ ]+$")))
					| (!(fullMatch(r, temp, "^[^ <]+ bind [^ ]+ [^ ]+$")))) {
                    /* ok */
                }
                else {
                    /* error, exit */
                    sprintf(errtxt_buf, "Syntax error in tag: '%s'", temp);
                    message_client('F', "buildSQLRecord()", errtxt_buf);
                    free(r);
                    free(modifiedclause);
                    free(temp);
                    return E_FATAL;
                }

                paramtype[0]='\0';
                fu[0]='\0';
                variablename[0]='\0';
                field_name[0]='\0';

                /* read the variables from the line */
                sprintf(format, "%%%ds %%%ds %%%ds %%%ds",
                        NAME_LENGTH-1, NAME_LENGTH-1, NAME_LENGTH-1,
                        NAME_LENGTH-1);
                paramcount = sscanf(temp, format, paramtype, fu,
                                    variablename, field_name);

                if ((paramcount == 4) && (strcmp(fu, "bind") == 0)) {
                    strncat(modifiedclause, field_name, strlen(field_name));
                }
                else {
                    modifiedclause[strlen(modifiedclause)] = '?';
                    modifiedclause[strlen(modifiedclause)] = '\0';
                }

                f = s->sql_func_list;
                prev_f = f;
                while (f != NULL) {
                    prev_f = f;
                    f = f->func_next;
                }
                if (prev_f == NULL) {
                    s->sql_func_list = (func_t*)malloc(sizeof(func_t));
                    if (s->sql_func_list == NULL) {
                        message_client('F', "buildSQLRecord()",
                                       "Dynamic memory allocation failed");
                        free(temp);
                        free(modifiedclause);
                        free(r);
                        return E_FATAL;
                    }
                    f = s->sql_func_list;
                }
                else {
                    prev_f->func_next = (func_t*)malloc(sizeof(func_t));
                    if (prev_f->func_next == NULL) {
                        message_client('F', "buildSQLRecord()",
                                       "Dynamic memory allocation failed");
                        free(s->sql_func_list);
                        free(temp);
                        free(modifiedclause);
                        free(r);
                        return E_FATAL;
                    }
                    f = prev_f->func_next;
                }
                f->func_next = NULL;
                f->func_var_value = calloc(16, sizeof(char));
                if (f->func_var_value == NULL) {
                    message_client('F', "buildSQLRecord()",
                                   "Dynamic memory allocation failed");
                    free(s->sql_func_list);
                    free(prev_f->func_next);
                    free(r);
                    free(modifiedclause);
                    return E_FATAL;
                }
                f->func_var_value_num = 0;

                /* assign the function */
                if (strcmp(fu, "rnd") == 0) {
                    f->func_function = rnd_client;
                    f->function_name = e_rnd;
                }
                else if (strcmp(fu, "rndstr") == 0) {
                    f->func_function = rndstr_client;
                    f->function_name = e_rndstr;
                }
                else if (strcmp(fu, "value") == 0) {
                    f->func_function = NULL;
                    f->function_name = e_value;
                }
                else if (strcmp(fu, "bind") == 0) {
                    f->func_function = NULL;
                    f->function_name = e_bindcol;
                }
                strncpy(f->func_type, paramtype, NAME_LENGTH);
                if (paramcount >= 3) {
                    /* bind */
                    strncpy(f->func_var_name, variablename, NAME_LENGTH);
                }
                else {
                    strcpy(f->func_var_name,"");
                }
                offset = offset + pos2 + 1;
                free(temp);
                temp = NULL;
            }
        }
        /* remaining part of the clause */
        strncat(modifiedclause, clause + offset, strlen(clause)-offset);
        /* clean up */
        removeEscapeCharacters(modifiedclause, strlen(modifiedclause));
        *newclause = modifiedclause;
        free(temp);
        free(r);
        return 0;
}

/*#***********************************************************************\
 *
 *      isAcceptedError
 *
 * Determines if error condition is expected as defined by
 * transaction mix file.
 *
 * Parameters:
 *      tr
 *          transaction pointer (transaction contains list of
 *          accepted errorcodes)
 *
 *      hstmt
 *          statement handle
 *
 * Return value:
 *      1 - if the error can be accepted
 *      0 - if the error cannot be accepted
  */
static int isAcceptedError(trans_t* tr, SQLHSTMT hstmt)
{
        SQLSMALLINT i;
        for (i = 1; ; ++i) {           
            char sqlState[SQL_SQLSTATE_SIZE + 1];
            SQLINTEGER nativeError;
            sqlerror_t *error;
            
            if (!SQL_SUCCEEDED(SQLGetDiagRec(
                                       SQL_HANDLE_STMT, hstmt,
                                       i, CHAR2SQL(sqlState),
                                       &nativeError,
                                       NULL, 0, NULL))) {
                break;
            }
            
            /* ignore warning codes */
            if (strncmp(sqlState, "01xxx", 2) == 0) {
                continue;
            }            
            error = tr->trans_errors;
            
            /* test to recognize ODBC (SQLSTATE) and native (naterr)
               error codes */
            while (error != NULL) {
                if ((strcmp(error->error_code, sqlState) == 0)
                    || (atoi(error->error_code) == nativeError)) {
#ifdef _DEBUG
                    error->error_amount++;
#endif
                    break;
                }                
                error = error->error_next;
            }            
            /* the error was not found from the accepted error codes list */
            if (error == NULL) {
                return 0;
            }
        }        
        return 1;
}

/*##**********************************************************************\
 *
 *      getvalue
 *
 * Returns value of the named variable
 *
 * Parameters:
 *      client
 *          pointer to client struct
 *
 *      parameter
 *          structure containing the variable name
 *
 * Return value:
 *      0  - success
 *     !0  - value not found
 */
int getvalue(client_t* client, func_t *parameter)
{
        var_t *variable;
        /* Iterate through the variables to find the variable
           we are searching for */
        for (variable = client->variables; variable != NULL;
             variable = variable->var_next) {
            if (strcmp(parameter->func_var_name, variable->var_name) == 0) {
                /* the variable found */
                if (getParamType(parameter->func_type) == SQL_VARCHAR) {
                    /* string */
                    strcpy((char*)parameter->func_var_value,
                           (const char*)variable->var_value);
                }
                else {
                    /* number */
                    parameter->func_var_value_num = variable->var_value_num;
                }
                return 0;
            }
        }
        /* not found */
        parameter->func_var_value = (void*)(-1*INT_MAX);
        parameter->func_var_value_num = -1*INT_MAX;
        return -1;
}

/*##**********************************************************************\
 *
 *      storeGlobalVariable
 *
 * Stores a value for a named variable into global variable list.
 *
 * Parameters:
 *      client
 *          pointer to client struct
 *
 *      param
 *          structure containing the variable
 *
 * Return value:
 *      none
 */
void storeGlobalVariable(client_t* client, func_t* param)
{
        /* pointer to a record in variable list */
        var_t *va;
        var_t *prev_va;
        
        /* if there is no function, do nothing */
        if (strcmp(param->func_var_name, "") != 0) {
            /* set the pointer to the first entry in the list */
            va = client->variables;
            prev_va = NULL;
            
            /* search for the right position */
            while (va != NULL) {
                if (strcmp(va->var_name, param->func_var_name) == 0) {
                    /* an entry for this variable name already
                       exists in the list */
                    break;
                }
                /* update pointers */
                prev_va = va;
                va = va->var_next;
            }
            
            if (va == NULL) {
                /* an entry was not found, so create a new entry at the
                   end of the list */
                va = (var_t*) malloc (sizeof(var_t));
                va->var_next = NULL;
                if (client->variables == NULL) {
                    /* the very first entry, so set the global variable list
                       pointer to it */
                    client->variables = va;
                }
                if (prev_va != NULL) {
                    prev_va->var_next = va;
                }
            }
            /* copy the value */
            strncpy(va->var_name, param->func_var_name, NAME_LENGTH);
            va->var_value = param->func_var_value;
            va->var_value_num = param->func_var_value_num;
        }
}

/*##**********************************************************************\
 *
 *      message_client
 *
 * Special message method for the client module.
 *
 * Parameters:
 *      errortype 
 *          type (verbosity) of error message 
 *
 *      location
 *          descriptive location of the error
 *
 *      errortext
 *          error message
 *
 * Return value:
 *          0  - success
 *         !0  - error
 */
void message_client(char errortype, const char *location,
                    const char *errortext)
{
        /* write to a file*/
        writeLog(errortype, errortext);
}

/*#***********************************************************************\
 *
 *      checkError
 *
 *
 * Classifies the error condition and determines which
 * kind of future execution is possible.
 *
 * Parameters:
 *      rc
 *          ODBC error code
 *
 *      handleType 
 *          type of ODBC handle in 'handle' argument
 *
 *      handle 
 *          ODBC handle to examine
 *
 *      tr_name
 *          name of transaction to include in error messages
 *
 *      location 
 *          location information to include in error messages
 *
 *      transaction 
 *          pointer to transaction struct
 *
 * Return value:
 *      E_OK    transaction shall continue
 *      E_ERROR transaction shall abort
 *      E_FATAL the whole test run shall abort
 */
static int checkError(SQLRETURN rc, SQLSMALLINT handleType, SQLHANDLE handle,
                      const char* tr_name, const char* location,
                      transaction_t* transaction)
{
        SQLSMALLINT i;
        int severity = SQL_SUCCEEDED(rc) ? E_OK : E_ERROR;
        int isSolidDB = (transaction != NULL
                         && (transaction->connection->db == DB_SOLID));
        
        switch (rc) {
            case SQL_NO_DATA:
                /* this code should not be passed here */
                message_client('W', location, "SQL_NO_DATA");
                break;

            case SQL_ERROR:
            case SQL_SUCCESS_WITH_INFO:
                for (i = 1; ; ++i) {
                    char sqlState[SQL_SQLSTATE_SIZE + 1];
                    SQLINTEGER nativeError;
                    char errorMessage[1024]; /* AUTOBUF */
                    SQLSMALLINT length;
                    int localSeverity = E_OK;
                    char errtxt_buf[1024]; /* AUTOBUF */
                    char location_buf[1024]; /* AUTOBUF */

                    if (!SQL_SUCCEEDED(SQLGetDiagRec(handleType,
                                                     handle,
                                                     i,
                                                     CHAR2SQL(sqlState),
                                                     &nativeError,
                                                     CHAR2SQL(errorMessage),
                                                     sizeof(errorMessage)/
                                                     sizeof(errorMessage[0]),
                                                     &length) ) ) {
                        break;
                    }

                    /* check for SolidDB specific errors */
                    if (isSolidDB) {
                        /* check for fail-over error code */
                        if (nativeError == E_SSAC_CONNECT_EXPECT_ROLLBACK) {
                            localSeverity = E_ERROR;
                            transaction->failover = 1;
                        }
                        
                    }
                    /* check for generic error states */
                    if (localSeverity == E_OK) {
                        /* check for transaction level errors
                         * in case of transaction level error further
                         * execution is possible */
                        
                        if (strncmp(sqlState, "01xxx", 2) == 0) { /* warning */
                            localSeverity = E_OK;
                        } else {                            
                            if (tr_name != NULL) {
                                sprintf(location_buf, "%s: %s",
                                        tr_name, location);
                            }
                            else {
                                sprintf(location_buf, "%s", location);
                            }

                            sprintf(errtxt_buf, "%s %d %s %s: \n", location,
                                    (int)nativeError, errorMessage, sqlState);
                            message_client('E', NULL, errtxt_buf);
                            assert(severity >= E_ERROR);
                            
                            if (strncmp(sqlState, "22xxx", 2) == 0
/* data exception */
                                || strncmp(sqlState, "23xxx", 2) == 0
/* integrity constraint violation */
                                || strncmp(sqlState, "40xxx", 2) == 0) {
/* transaction failure */
                                
                                localSeverity = E_ERROR;
                            }
                            else {
                                localSeverity = E_FATAL;
                            }
                        }
                    }
                    
                    sprintf(errtxt_buf, "%d %s %s: \n", (int)nativeError,
                            errorMessage, sqlState);

                    if (tr_name != NULL) {
                        sprintf(location_buf, "%s: %s", tr_name, location);
                    }
                    else {
                        sprintf(location_buf, "%s", location);
                    }
                    
                    if (localSeverity >= E_FATAL) {
                        if(isSolidDB && nativeError
                           == E_SSAC_CONNECT_EXPECT_ROLLBACK) {
                            message_client('I', NULL,
                                           "Connection switch detected.");
                            localSeverity = E_ERROR;
                        } else {
                            message_client('F', location_buf, errtxt_buf);
                        }
                    }
                    else
                    if (localSeverity >= E_ERROR) {
                        if(isSolidDB && nativeError
                           == E_SSAC_CONNECT_EXPECT_ROLLBACK) {
                            message_client('I', NULL,
                                           "Connection switch detected.");
                        } else {
                            message_client('E', location_buf, errtxt_buf);
                        }
                    }
                    else {
                        message_client('W', location_buf, errtxt_buf);
                    }

                    if (rc == SQL_ERROR && localSeverity > severity) {
                        severity = localSeverity;
                    }
                }
                break;
        }
        if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
            assert(severity == E_OK);
        }
        
        return severity;
}

/*#***********************************************************************\
 *
 *      checkErrorS
 *
 * Wrapper for checkError to be used with ODBC statement handles.
 *
 * Parameters:
 *      rc
 *          ODBC error code
 *
 *      hstmt
 *          ODBC statement handle to examine
 *
 *      tr_name
 *          name of transaction to include in error messages
 *
 *      location
 *          location information to include in error messages
 *
 *      transaction
 *          pointer to transaction struct
 *
 * Return value:
 *      return value from checkError()
 */
static int checkErrorS(SQLRETURN rc, SQLHSTMT hstmt, const char* tr_name,
                       const char* location, transaction_t* transaction)
{

        return checkError(rc, SQL_HANDLE_STMT, hstmt, tr_name, location,
                          transaction);
}

/*#***********************************************************************\
 *
 *      closeStatements
 *
 * Closes all open transaction handles for all transactions.
 *
 * Parameters:
 *      client 
 *          pointer to client struct 
 *
 * Return value:
 *      none
 */
static void closeStatements(client_t* client)
{
        trans_t* tr;
        for (tr = client->tr_head; tr != NULL; tr = tr->trans_next) {
            
            sql_t* sqlrec;
            for (sqlrec = tr->trans_sql; sqlrec != NULL;
                 sqlrec = sqlrec->sql_next) {
                
                if (sqlrec->sql_statement != SQL_NULL_HSTMT) {                    
                    SQLRETURN rc;
                    rc = SQLFreeHandle(
                            SQL_HANDLE_STMT,
                            sqlrec->sql_statement);
                    
                    checkErrorS(rc, sqlrec->sql_statement,
                                NULL, "SQLFreeHandle()", NULL);
                    
                    sqlrec->sql_statement = SQL_NULL_HSTMT;
                }
            }
        }
}

/*#***********************************************************************\
 *
 *      prepareTransaction
 *
 * Conditionally prepares all statements for a transaction and binds
 * parameter slots.
 *
 * Parameters:
 *      db_transaction
 *          pointer to transaction struct that contain connection data
 *
 *      transaction 
 *          pointer to trans struct that contains transaction data
 *
 * Return value:
 *      E_OK    - transaction shall continue
 *      E_ERROR - transaction shall abort
 *      E_FATAL - the whole test run shall abort
 */
static int prepareTransaction(transaction_t* db_transaction,
                              trans_t* transaction)
{
        SQLRETURN err;
        int severity = E_OK;
        sql_t *sqlrec;
        func_t *param;
        int param_nr, bind_nr;
        int param_value_type;
        
        /* Prepare all SQL statements inside the transaction */
        /* ... but not those which have already been prepared */
        sqlrec = transaction->trans_sql;
        
        while (sqlrec != NULL) {
            if (sqlrec->sql_statement == SQL_NULL_HSTMT) {
                
                err = SQLAllocHandle(SQL_HANDLE_STMT,
                                     db_transaction->connection->hdbc,
                                     &sqlrec->sql_statement);
                
                severity = checkErrorS(err, sqlrec->sql_statement,
                                       transaction->trans_name,
                                       "SQLAllocHandle()", db_transaction);
                if (severity >= E_ERROR) {
                    return severity;
                }
                
                /* Prepare the SQL command */
                err = SQLPrepare(sqlrec->sql_statement,
                                 CHAR2SQL(sqlrec->sql_clause),
                                 SQL_NTS);
                
                severity = checkErrorS(err, sqlrec->sql_statement,
                                       transaction->trans_name, "SQLPrepare()",
                                       db_transaction);
                if (severity >= E_ERROR) {
                    return severity;
                }
                
                param = sqlrec->sql_func_list;
                param_nr = 1;
                bind_nr = 1;
                
                /* Handle all the parameters we can find from the command */
                while (param != NULL) {
                    param_value_type = getParamType(param->func_type);
                    if (param->function_name == e_bindcol) {
                        /* this parameter is an output column to bind */
                        if (param_value_type == SQL_VARCHAR) {
                            err = SQLBindCol(sqlrec->sql_statement,
                                             bind_nr,
                                             getValueType(param->func_type),
                                             param->func_var_value,
                                             SUBNBR_LENGTH+1,
                                             NULL);
                        }
                        else {
                            /* Not a SQL_VARCHAR type column */
                            param->func_var_value_num = 0;
                            err = SQLBindCol(sqlrec->sql_statement,
                                             bind_nr,
                                             getValueType(param->func_type),
                                             &param->func_var_value_num,
                                             sizeof(param->func_var_value_num),
                                             NULL);
                        }
                        
                        severity = checkErrorS(err, sqlrec->sql_statement,
                                               transaction->trans_name,
                                               "SQLBindCol()",
                                               db_transaction);
                        if (severity >= E_ERROR) {
                            return severity;
                        }
                        
                        bind_nr++;
                    }
                    else {
                        /* this parameter is an input column to bind */
                        if (param_value_type == SQL_VARCHAR) {
                            err = SQLBindParameter(sqlrec->sql_statement,
                                                   param_nr,
                                                   SQL_PARAM_INPUT,
                                                   getValueType(
                                                           param->func_type),
                                                   param_value_type,
                                                   getColumnSize(
                                                           param->func_type),
                                                   0,
                                                   param->func_var_value,
                                                   0,
                                                   NULL);

                    }
                        else {
                            /* Not a SQL_VARCHAR type column */
                            err = SQLBindParameter(sqlrec->sql_statement,
                                                   param_nr,
                                                   SQL_PARAM_INPUT,
                                                   getValueType(
                                                           param->func_type),
                                                   param_value_type,
                                                   0,
                                                   0,
                                                   &param->func_var_value_num,
                                                   0,
                                                   NULL);
                        }
                        param_nr++;
                        severity = checkErrorS(err, sqlrec->sql_statement,
                                               transaction->trans_name,
                                               "SQLBindParameter()",
                                               db_transaction);
                        if (severity >= E_ERROR) {
                            return severity;
                        }
                    }
                    param = param->func_next;
                }
            }
            sqlrec = sqlrec->sql_next;
        } /* while sqlrec ... */
        
        return severity;
}

/*#***********************************************************************\
 *
 *      runTransaction
 *
 * Executes a single prepared transaction. Runs all
 * SQL clauses until error condition is encountered.
 *
 * Parameters:
 *      client 
 *          pointer to client struct
 *
 *      transaction
 *          transaction structure
 *
 *      transaction_timer 
 *          timer to use for latency measurements
 *
 *      transaction_time
 *          total execution time
 *
 *      rollbackOnError
 *          flag to indicate that we hit an error demanding that
 *          we do a rollback
 *
 *      acceptedError 
 *          flag to indicate that transaction encountered an
 *          expected error and should be counted in results.
 *
 *      db_transaction 
 *          
 *
 * Return value:
 *      E_OK    transaction shall continue
 *      E_ERROR transaction shall abort
 *      E_FATAL the whole test run shall abort
 */
static int runTransaction(client_t* client, trans_t* transaction,
                          struct timertype_t* transaction_timer,
                          __int64* transaction_time,
                          int* rollbackOnError, int* acceptedError,
                          transaction_t* db_transaction)
{
        SQLRETURN err;
        int severity = E_OK;
        sql_t *sqlrec;
        char errtxt_buf[1024]; /* AUTOBUF */
        int sqli = 0;
        
        *acceptedError = 0;
        *rollbackOnError = 0;
        
        /* find the first sql clause from the transaction that got
           selected to be run against the target database */
        
        /* run all SQL statements within the transaction */
        sqlrec = transaction->trans_sql;
        while (sqlrec != NULL && severity < E_ERROR) {
            
            __int64 temp;
            func_t *param;
            int contains_select_bind;
            int no_data_reported = 0;
            
            param = sqlrec->sql_func_list;
            contains_select_bind = 0;
            
            /* pre-processing */
            while (param != NULL) {
                if (param->function_name == e_value) {
                    /* get previously stored value for the parameter */
                    if (getvalue(client, param) == -1) {
                        sprintf(errtxt_buf, "Called undefined global "
                                "variable: %s",
                                param->func_var_name);
                        message_client('E', "", errtxt_buf);
                        return E_ERROR; /* TODO E_FATAL ???? */
                    }
                }
                else if (param->function_name != e_bindcol) {
                    /* process a function */
                    if (getParamType(param->func_type) == SQL_VARCHAR) {
                        (*param->func_function)(client, param->func_type,
                                                (char*)param->func_var_value);
                    }
                    else {
                        param->func_var_value_num = (int)
                            (*param->func_function)(client,
                                                    param->func_type, NULL);
                    }
                    if (strcmp(param->func_var_name, "") != 0) {
                        /* store the result into global a variable */
                        storeGlobalVariable(client, param);
                    }
                }
                else {
                    /* bind result column, do nothing right now */
                    contains_select_bind = 2;
                }
                param = param->func_next;
            }
            
            if ((!contains_select_bind)
                && (strstr(sqlrec->sql_clause, "SELECT") != 0)) {
				/* still needs to be fetched */
                contains_select_bind = 1;
            }
            
            sprintf(errtxt_buf, "Executing SQL clause #%d", sqli + 1);
            message_client('X', "runTest()", errtxt_buf);
            
            /* Start measuring transaction time */
            startTimer(transaction_timer);
            
            /* run the SQL statement */
            err = SQLExecute(sqlrec->sql_statement);
            
            if (!SQL_SUCCEEDED(err)
                && isAcceptedError(transaction, sqlrec->sql_statement)) {
                /* we hit an error that is defined as an accepted error
                   in transaction file */
                message_client('X', "runTest()", "Accepted error encountered");
                
                *acceptedError = 1;
                *rollbackOnError = 1;
                severity = E_ERROR;
            }
            else {
                if (!contains_select_bind
                    && err == SQL_NO_DATA) {
					no_data_reported = 1;
					err = SQL_SUCCESS;
                }
                severity = checkErrorS(err, sqlrec->sql_statement,
                                       transaction->trans_name, "SQLExecute()",
                                       db_transaction);
            }
            
            if (severity < E_ERROR) {
                /* executed ok, continue normally */
                SQLLEN updating = 0;
                SQLLEN rowCount;
                
                if (!(contains_select_bind)) {                    
                    if (no_data_reported) {                        
                        rowCount = 0;
                    } else {
                        /* get affected rows */
                        err = SQLRowCount (sqlrec->sql_statement, &rowCount);
                        severity = checkErrorS(err, sqlrec->sql_statement,
                                               transaction->trans_name,
                                               "SQLRowCount()", db_transaction);
                    }                    
                    if (severity < E_ERROR) {
                        sprintf(errtxt_buf, "%d rows affected", (int)rowCount);
                        message_client('X', "runTest()", errtxt_buf);
                    }                    
                    if (rowCount > 0) {
                        updating = 1;
                    }
                }
                else {                    
                    /* fetch if the clause contains SELECT or SELECT which
                       binds to global variable */                    
                    for (rowCount = 0; ; ++rowCount) {                        
                        err = SQLFetch(sqlrec->sql_statement);
                        if (err == SQL_NO_DATA) {
                            break;
                        }
                        
                        severity = checkErrorS(err, sqlrec->sql_statement,
                                               transaction->trans_name,
                                               "SQLFetch()", db_transaction);
                        if (severity >= E_ERROR) {
                            break;
                        }
                    }
                    
                    sprintf(errtxt_buf, "%d rows fetched", (int)rowCount);
                    message_client('X', "runTest()", errtxt_buf);
                    
                    err = SQLCloseCursor(sqlrec->sql_statement);
                    checkErrorS(err, sqlrec->sql_statement,
                                transaction->trans_name, "SQLCloseCursor()",
                                db_transaction);
                    
                    if (severity < E_ERROR
                        && rowCount != 0) {
                        
                        /* SELECT returned > 0 rows */
                        
                        /* Do not bind if sqlclause produced an error */
                        /* Put bound columns into global variable list */
                        if (contains_select_bind == 2) {
                            param = sqlrec->sql_func_list;
                            while (param != NULL) {
                                if (param->function_name == e_bindcol) {
                                    storeGlobalVariable(client, param);
                                }
                                param = param->func_next;
                            }
                        }
                    }
                }
                
                if (severity < E_ERROR) {
                    
#ifdef _DEBUG
                    if (rowCount > 0) {
                        sqlrec->sql_ok_amount++;
                    }
                    else {
                        sqlrec->sql_no_rows_amount++;
                    }
#endif
                    if (rowCount == 0 && !contains_select_bind) {
                        severity = E_ERROR;
                        *rollbackOnError = 1;
                    } /* rollback update transacion on "found no data" */
                    else if (updating) {
                        *rollbackOnError = 1;
                    } /* otherwise (GET transactions) commit */
                }
            }
            
            /* The SQL command was executed.
               Stop the transaction timing */
            stopTimer(transaction_timer);
            readTimer(transaction_timer, &temp);
            *transaction_time += temp;
            
            sqlrec = sqlrec->sql_next;
            sqli++;
        } /* while sqlrec != NULL && severity < E_ERROR */

        /* (that is, continue if we still have SQL commands to execute
           within the transaction */
        /* AND we did not run into error (even if it were accepted) */

        return severity;
}

/*#***********************************************************************\
 *
 *      startTransaction
 *
 * Initializes transaction struct
 *
 * Parameters:
 *      transaction 
 *          pointer transaction struct to use
 *
 *      connection 
 *          pointer to connection info struct
 *
 * Return value:
 *      E_OK    commit was successful
 *      E_ERROR the transaction shall abort
 *      E_FATAL the whole test run shall abort
 */
static void startTransaction(transaction_t* transaction,
                             const connection_t* connection)
{
        transaction->failover = 0;
        transaction->connection = connection;
}

/*##**********************************************************************\
 *
 *      commitTransaction
 *
 * Commits a transaction.
 *
 * Parameters:
 *      transaction
 *          pointer to transaction struct
 *
 *      tr_name
 *          name of the transaction
 *
 * Return value:
 *      checkError() return value
 */
int commitTransaction(transaction_t* transaction, const char* tr_name)
{
        int severity;
        SQLRETURN err;
        
        assert(transaction && !transaction->failover);
        
        err = SQLEndTran(SQL_HANDLE_DBC, transaction->connection->hdbc,
                         SQL_COMMIT);
        severity = checkError(err,
                              SQL_HANDLE_DBC, transaction->connection->hdbc,
                              tr_name, "COMMIT",
                              transaction);
    return severity;
}

/*##**********************************************************************\
 *
 *      rollbackTransaction
 *
 * Rollsbacks a transaction.
 *
 * Parameters:
 *      transaction
 *          pointer to transaction struct
 *
 *      tr_name 
 *          name of the transaction
 *
 * Return value:
 *      checkError() return value
 */
int rollbackTransaction(transaction_t* transaction, const char* tr_name)
{
        int rollback_status;
        SQLRETURN err;
        
        assert(transaction && !transaction->failover);
        err = SQLEndTran(SQL_HANDLE_DBC, transaction->connection->hdbc,
                         SQL_ROLLBACK);
        rollback_status = checkError(err,
                                     SQL_HANDLE_DBC,
                                     transaction->connection->hdbc,
                                     tr_name, "ROLLBACK",
                                     transaction);

        return rollback_status;
}

/*##**********************************************************************\
 *
 *      rollbackOnFailover
 *
 * Rollbacks a transaction in case of failover
 *
 * Parameters:
 *      hdbc 
 *          ODBC connection handle
 *
 *      tr_name 
 *          name of the transaction
 *
 *      client 
 *          pointer to client struct
 *
 * Return value:
 *      return value of checkError()
 */
int rollbackOnFailover(SQLHDBC hdbc, const char* tr_name, client_t* client)
{
        SQLRETURN err;
        int rollback_status = E_ERROR; /* value not used */
        int i;
        
        /* try several times */
        for (i = 0; i != FAILOVER_RETRIES; i++) {            
            err = SQLEndTran(
                    SQL_HANDLE_DBC,
                    hdbc,
                    SQL_ROLLBACK);            
            rollback_status = checkError(err,
                                         SQL_HANDLE_DBC, hdbc,
                                         tr_name, "ROLLBACK AFTER FAILOVER",
                                         NULL);  /* failover checking is
                                                    no longer performed */
            if (rollback_status == E_OK) {
                break;
            }
            /* wait some time before retry */
            msSleep(100);
        }
        
        if (rollback_status == E_OK) {
            /* failover done and rollback succeeded */
            /* the following is needed for connection-level
               failure transparency */
            
            /* statements must be prepared again, so clean up them */
            if (client != NULL)
                closeStatements(client);
            
            /* we have to set autocommit off again */
            err = SQLSetConnectAttr(
                    hdbc,
                    SQL_ATTR_AUTOCOMMIT,
                    (SQLPOINTER) SQL_AUTOCOMMIT_OFF,
                    0);
            
            checkError(err,
                       SQL_HANDLE_DBC, hdbc,
                       tr_name, "SQL_AUTOCOMMIT_OFF",
                       NULL);
        }
        
        return rollback_status;
}

/*#***********************************************************************\
 *
 *      endTransaction
 *
 *      Depending on transaction_status commits or rolls
 *      back a transaction and recovers after possible
 *      connection fail-over.
 *
 * Parameters:
 *      transaction
 *          pointer to transaction struct
 *
 *      transaction_status
 *          current transaction status
 *
 *      tr_name 
 *          name of transaction for diagnostic purpose
 *
 *      location 
 *          location information for console messages
 *
 *      commit_timer 
 *          timer used to measure transaction commit time
 *
 *      client 
 *          pointer to client struct
 *
 * Return value:
 *       E_OK    - rollback was made and the test may continue
 *       E_ERROR - the transaction shall abort
 *       E_FATAL - the whole test run shall abort
 */
static int endTransaction(transaction_t* transaction,
                          int transaction_status, const char* tr_name,
                          const char* location,
                          struct timertype_t* commit_timer, client_t* client)
{
        if (transaction_status < E_ERROR) {
            /* commit transaction */
            message_client('X', location, "Committing transaction");
            /* Start measuring transaction time to measure also the time
               spent in COMMIT */
            if (commit_timer != NULL) {
                startTimer(commit_timer);
            }
            transaction_status = commitTransaction(transaction, tr_name);
            /* Stop the transaction timing */
            if (commit_timer != NULL) {
                stopTimer(commit_timer);
            }
        }
        
        if (transaction_status >= E_ERROR && !transaction->failover) {
            int rollback_status;
            
            /* roll back to cancel previous actions */
            message_client('X', location, "Rolling transaction back");
            rollback_status = rollbackTransaction(
                transaction, tr_name);
            if (rollback_status >= E_ERROR && !transaction->failover) {
                /* cannot survive rollback failure */
                transaction_status = E_FATAL;
            }
            else if (rollback_status > transaction_status) {
                transaction_status = rollback_status;
            }
        }

        if (transaction_status >= E_ERROR
            && transaction->failover) {
            int rollback_severity;
            message_client('D', location,
                           "Rolling transaction back after fail-over");
            rollback_severity = rollbackOnFailover(
                transaction->connection->hdbc, tr_name, client);
            if (rollback_severity >= E_ERROR) {
                /* cannot survive rollback failure */
                transaction_status = E_FATAL;
            }
        }
        return transaction_status;
}

/*##**********************************************************************\
 *
 *      runTests
 *
 * Performs the actual benchmark. The main loop is run
 * as long as the benchmark is supposed to last (either
 * elapased time or number of transactions to be executed
 * is used). Each iteration of the main loop consists of :
 *	1) randomly selecting a transaction to be run next
 *  2) Preparing un-prepared commands in the target db
 *  3) binding the command parameters when needed
 *  4) finding values for the command variables
 *  5) executing the commands
 *  6) fetching return values from the target db (if the
 *		command type that requires)
 *  7) communicating the results to the Statistics module.
 *
 * Parameters:
 *      testdb
 *          the test database handle
 *      transactions
 *           names and probabilities of the transactions.
 *           Probability is the next element after the name
 *           of transaction
 *      tr_choices
 *           amount of the transactions to choose from
 *      rampup_time
 *			 warm up time (in minutes)
 *      run_time
 *           time (in minutes) to run the transactions. Note that
 *			 warmup time is included in this figure
 *      DBSchemaName
 *           Database schema name
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int runTests(client_t* client, char *transactions[], int tr_choices,
             int rampup_time, int run_time, char* DBSchemaName, int operation_mode)
{
        SQLRETURN   err = SQL_SUCCESS;
        trans_t     *transaction;
        /* calculate milliseconds from minutes */
        int         testTime = 1000 * 60 * run_time;
        __int64     elapsed = 0;
        __int64     rampupTime = 1000 * 60 * rampup_time;
        char        *tr_name = NULL;
        int         i;
        int         prob;
        int         sum;
        int         tr_run = 0;
        char        errtxt_buf[256];
		int			ret = 0;
		int			status = 0;
        unsigned int i_dedicated_transaction;

        int         prev_mqth_time_slot = 0;
        int db1_mqth = 0;
        int db2_mqth = 0;
        struct timertype_t transaction_timer;
        connection_t* connection = &client->connection;

#ifdef _DEBUG
        sqlerror_t  *error;
        int         *tr_realized;
        tr_realized = (int*) calloc(tr_choices,sizeof(int));
#endif

        initTimer(&transaction_timer, TIMER_MICROSECOND_SCALE);

#ifndef NO_TPS_TABLE
        if (reportTPS) {
            deleteTPSrow(connection, client->clientID, DBSchemaName);
            /* create client row in TPS table */
            if (insertTPSrow(connection->hdbc, client->clientID,
                             DBSchemaName)
                >= E_ERROR) {
                return E_FATAL;
            }
        }
#endif
        sprintf(errtxt_buf,
                "Feeding transactions for %d seconds", testTime/1000);
        message_client('I', NULL, errtxt_buf);
#ifndef NO_TPS_TABLE
        /* allocate and prepare UPDATE TPS statement */
        if (reportTPS) {
            ret = SQLAllocHandle(SQL_HANDLE_STMT,
                                 connection->hdbc,
                                 &connection->tps.stmtUpdate);
            
            status = checkError(ret, SQL_HANDLE_DBC, connection->hdbc,
                                "TPS update", "Allocate handle", NULL);

            if (status != E_OK) {
#ifdef _DEBUG
                sprintf(errtxt_buf, "UPDATE TPS handle allocation failed");
                message_client('I', "runTests()", errtxt_buf);
#endif
                return E_FATAL;
            }

            status = prepareUpdateTPSrow(&connection->tps,
                                         client->solid_ha_stat,
                                         DBSchemaName);
            if (status != E_OK) {
#ifdef _DEBUG
                sprintf(errtxt_buf, "Prepare UPDATE TPS failed");
                message_client('I', "runTests()", errtxt_buf);
#endif
                return E_FATAL;
            }
        }
#endif /* NO_TPS_TABLE */

        i_dedicated_transaction = 2 * ((client->clientID - 1) % tr_choices);
        
        /* main loop; loop until the test time has passed */        
        do {
            transaction_t db_transaction;
            __int64 transaction_time = 0;
            int severity;
            int status_for_commit;
            int rollbackOnError = 0;   /* transaction has updates
                                          to roll back on failure */
            int acceptedError = 0;
#ifdef TC_COUNT
            int solid_connection_used; /* 0 primary, 1 secondary,
                                          other unknown */
#endif

            startTransaction(&db_transaction, connection);

            tr_run++;

            if (operation_mode == RUN_DEDICATED) {
                /* always run the same transaction */
                tr_name = transactions[i_dedicated_transaction];
#ifdef _DEBUG
                tr_realized[i_dedicated_transaction]++;
#endif                
            } else {
                /* normal run */
                /* randomly pick the next transaction */
                if (tr_choices > 1) {
                    prob = get_random(&client->rand, 1, 100);
                    sum = 0;
                    for (i = 0; i < tr_choices * 2 ; i = i+2) {
                        sum = sum + atoi(transactions[i+1]);
                        if (prob <= sum) {
                            tr_name = transactions[i];
#ifdef _DEBUG
                            tr_realized[i/2]++;
#endif
                            break;
                        }
                    }
                }
                else {
                    tr_name = transactions[0];
#ifdef _DEBUG
                    tr_realized[0]++;
#endif
                }
            }

            /* find the transaction from the transaction list */
            for (transaction = client->tr_head; transaction != NULL;
                 transaction = transaction->trans_next) {
                if (strcmp(tr_name, transaction->trans_name) == 0) {
                    break;
                }
            }

            if (transaction == NULL) {
                sprintf(errtxt_buf,
                        "An undefined transaction %s was called...aborting",
                        tr_name);
                message_client('E', "", errtxt_buf);
                severity = E_FATAL;
                return severity; /* TODO break */
            }
            
            sprintf(errtxt_buf,
                    "Running transaction %s",
                    transaction->trans_name);
            message_client('X', "runTests()", errtxt_buf);

            severity = prepareTransaction(&db_transaction, transaction);
            if (severity < E_ERROR) {                

                /* running the actual transaction */
                severity = runTransaction(client,
                                          transaction,
                                          &transaction_timer,
                                          &transaction_time,
                                          &rollbackOnError,
                                          &acceptedError,
                                          &db_transaction);
#ifdef TC_COUNT
                
                solid_connection_used = -1;
                if ((severity < E_ERROR || (severity == E_ERROR
                                            && acceptedError))
                    && client->solid_ha_stat) {

                    SQLRETURN rc2;
                    char stringBuf[256]; /* AUTOBUF */

                    assert(connection->db == DB_SOLID);

                    rc2 = SQLGetConnectAttr(
                            connection->hdbc,
                            SQL_ATTR_TC_WORKLOAD_CONNECTION, stringBuf,
                            sizeof(stringBuf),
                            NULL);

                    if (checkError(
                                rc2,
                                SQL_HANDLE_DBC, /*sqlrec->sql_statement*/
                                connection->hdbc,
                                transaction->trans_name,
                                "Get SQL_ATTR_TC_WORKLOAD_CONNECTION failed",
                                NULL) < E_ERROR) {

                        trim_connect_string(stringBuf);
#ifdef _DEBUG
                        sprintf(errtxt_buf,
                                "Workload:  %s",stringBuf);
                        message_client('D', NULL, errtxt_buf);
                        message_client('D', NULL,
                                       connection->solid_connection_one);
                        message_client('D', NULL,
                                       connection->solid_connection_two);
                        message_client('D', NULL, "-------");
#endif

                        if (strcmp(stringBuf,
                                   connection->solid_connection_one) == 0) {
                            solid_connection_used = 0;
                        } else {
                            /* assert would not work:
                               solid_connection_two is sometimes empty)
                               assert(strcmp(stringBuf,
                               solid_connection_two) == 0);
                            */
                            solid_connection_used = 1;
                        }
                    }
                }
#endif /* TC_COUNT */
            }

            /* In order to COMMIT certain failed transactions
             * we replace input status to endTransaction()
             */
            status_for_commit = severity;

            if (severity == E_ERROR
                && !rollbackOnError
                && !db_transaction.failover) {

                /* Allow failed GET transaction to commit */
                status_for_commit = E_OK;
            }

            /* commit the transaction executed above */
            /* include transaction_timer so that can commit time will be included in latency */            
            status_for_commit = endTransaction(&db_transaction,
                                               status_for_commit,
                                               transaction->trans_name,
                                               "runTransaction()",
                                               &transaction_timer,
                                               client);
            
            if (status_for_commit > severity) {
                severity = status_for_commit;
            }

            if (severity < E_ERROR) {
                __int64     temp;

                /* add commit time to transaction time */
                readTimer(&transaction_timer, &temp);
                transaction_time += temp;                
                /* all successful transactions are counted */
                transaction->commitCount++;
            } else {
                /* we did a successful rollback */
                transaction->rollbackCount++;
            }
            
            if (severity < E_ERROR) {
                message_client('X', "runTests()", "Transaction is counted");

#ifdef TC_COUNT
                if (client->solid_ha_stat) {                    
                    if (solid_connection_used == 0) {        /* PRIMARY */
                        db1_mqth++;
                    } else if (solid_connection_used == 1) { /* SECONDARY */
                        db2_mqth++;
                    }
                }
#endif /* TC_COUNT */

                /* Store result to array */
                prev_mqth_time_slot = client->current_mqth_time_slot;
                /* Check if it is time to change the MQTh time slot */
                checkAndUpdateMQTHSlot(client);
                
                /* now we know which timeslot to use */
                client->throughput_data[client->current_mqth_time_slot]++;
                if (prev_mqth_time_slot != client->current_mqth_time_slot) {
#ifndef NO_TPS_TABLE
                    if (reportTPS) {
                        int tps_status;
                        tps_status =
                            updateRealtimeStats(client,
                                                client->throughput_data[
                                                        prev_mqth_time_slot],
                                                db1_mqth,
                                                db2_mqth);
                    }
#endif
#ifdef TC_COUNT
                    db1_mqth = 0;
                    db2_mqth = 0;
#endif
                }
            } else if (severity == E_ERROR && acceptedError) {
                /* ignore transactions that failed with an error on
                   ERRORS ALLOWED list */
                transaction->ignoredCount++;
            }
            
            /* read the timer (to be able to check when to stop the test) */
            readTimer(&client->global_timer, &elapsed);
            
            if ((client->testTimeOffset + elapsed) > rampupTime) {
                /* record response times only for transactions that are
                   calculated in MQTh */
                if (severity < E_ERROR) {
                    /* update response time array */
                    /* array contains amount of transactions */
                    /* index+1 is the response time */

#ifdef LATENCYBOUNDS
                    if (transaction_time > transaction->maxLatency) {
                        transaction->maxLatency = (int)transaction_time;
                    }
                    if (transaction_time < transaction->minLatency) {
                        transaction->minLatency = (int)transaction_time;
                    }
#endif
                    
#ifndef LINEAR_RESPONSE_SCALE
                    {
                        /* a simple binary search */
                        int low = 0;
                        int high = response_time_bound_count;
                        while (low != high) {
                            int mid = low + (high - low) / 2;
                            if (response_time_bounds[mid] < transaction_time)
                                low = mid + 1;
                            else
                                high = mid;
                        }

                        if (low < response_time_bound_count) {
                            transaction->response_times[low]++;
                        }
                    }
#else
                    if (transaction_time < MAX_RESP_TIME_SLOTS) {
                        transaction->response_times[transaction_time]++;
                    }
#endif
                }
            }

            if (severity >= E_FATAL) {
                return severity;
            }
        } while ( ( (client->testTimeOffset + elapsed) < testTime)
                  && (err == 0) );
        /* end of test run loop*/

#ifndef NO_TPS_TABLE
        /* free the UPDATE TPS statement handle */
        if (reportTPS) {
            ret = SQLFreeHandle(
                    SQL_HANDLE_STMT,
                    connection->tps.stmtUpdate);
            status = checkErrorS(
                    ret,
                    connection->tps.stmtUpdate,
                    "TPS update",
                    "Free statement",
                    NULL);
#ifdef _DEBUG
            if (status != E_OK) {
                sprintf(errtxt_buf,
                        "Failed to free the UPDATE TPS statement handle");
                message_client('I', "", errtxt_buf);
            }
#endif
            deleteTPSrow(connection, client->clientID, DBSchemaName);
        }

#endif /* NO_TPS_TABLE */

        /* benchmark run ends */
        sprintf(errtxt_buf, "Transactions executed: %d", tr_run);

        /* collect some statistics */
#ifdef _DEBUG
        for (i = 0; i < tr_choices ; i++) {
            sprintf(errtxt_buf, "%s: %d times", transactions[i*2],
                    tr_realized[i]);
            message_client('I', "", errtxt_buf);
        }
        free(tr_realized);
#endif
        for (transaction = client->tr_head;
             transaction != NULL;
             transaction = transaction->trans_next) {
            sql_t *sqlrec;
            
#ifdef LATENCYBOUNDS
            sprintf(errtxt_buf,
                    "Transaction: %s, commits: %d, rollbacks: %d, "
                    "ignoredcount: %d, "
                    "minLatency: %d, maxLatency: %d",
                    transaction->trans_name,
                    transaction->commitCount,          
                    transaction->rollbackCount,
                    transaction->ignoredCount,
                    transaction->minLatency,
                    transaction->maxLatency);
#else            
            sprintf(errtxt_buf,
                    "Transaction: %s, commits: %d, rollbacks: %d, "
                    "ignoredcount: %d",
                    transaction->trans_name,
                    transaction->commitCount,
                    transaction->rollbackCount,
                    transaction->ignoredCount);            
#endif
            
            if (detailedStatistics) {
                message_client('I', "", errtxt_buf);
            } else {
                message_client('D', "", errtxt_buf);
            }

#ifdef _DEBUG
            error = transaction->trans_errors;
            while (error != NULL) {
                sprintf(errtxt_buf,
                        "Transaction: %s, Error code: %s, Amount: %d",
                        transaction->trans_name,
                        error->error_code,
                        error->error_amount);

                message_client('I', "", errtxt_buf);
                error = error->error_next;
            }
#endif
            /* cleanup */
            sqlrec = transaction->trans_sql;
            i = 0;
            while (sqlrec != NULL) {
                i++;
#ifdef _DEBUG
                sprintf(errtxt_buf,
                        "Transaction: %s",
                        transaction->trans_name);
                message_client('I', "", errtxt_buf);
                sprintf(errtxt_buf,
                        "SQL clause #%d: found data: %d times, "
                        "found no data: %d times",
                        i,
                        sqlrec->sql_ok_amount,
                        sqlrec->sql_no_rows_amount);

                message_client('I', "", errtxt_buf);
#endif
                if (sqlrec->sql_statement) {
                    /* The statement was allocated. Free it */
                    err = SQLFreeHandle(SQL_HANDLE_STMT,
                                        sqlrec->sql_statement);
                    if (error_s(sqlrec->sql_statement, err, NULL)) {
                        message_client('E',
                                       "SQLFreeHandle()",
                                       "Error in SQLFreeHandle");
                        return E_ERROR;
                    }
                }
                sqlrec = sqlrec->sql_next;
            }
        }

        sprintf(errtxt_buf,
                "Client was run for: %d seconds",
                (int) elapsed/1000);
        message_client('D', "", errtxt_buf);
        return E_OK;
}

#ifndef NO_TPS_TABLE

/*##**********************************************************************\
 *
 *      prepareUpdateTPSrow
 *
 * Prepares and executes TPS table UPDATEs 
 *
 * Parameters:
 *      tps 
 *          Pointer to tpstable struct
 * 
 *      solid_ha_stat 
 *          flag that indicates if Solid HA is used
 *
 *      DBSchemaName 
 *          Database schema name
 *
 * Return value:
 *      checkError() return value
 */
int prepareUpdateTPSrow(tpstable_t* tps, int solid_ha_stat, char *DBSchemaName)
{
        int status;
        SQLRETURN ret;
        char stmt[512]; /* AUTOBUF */
        
        if (solid_ha_stat) {
            sprintf(stmt, "UPDATE %stps SET value=?, db1=?, db2=? WHERE id=?",
                    (*DBSchemaName == '\0' ? "" : DBSchemaName));
        } else {
            sprintf(stmt, "UPDATE %stps SET value=? WHERE id=?",
                    (*DBSchemaName == '\0' ? "" : DBSchemaName));
        }
        
        /* Prepare the SQL command */
		ret = SQLPrepare(tps->stmtUpdate, CHAR2SQL(stmt), SQL_NTS);
        status = checkError(ret, SQL_HANDLE_STMT, tps->stmtUpdate,
                            "TPS update", "Prepare UPDATE TPS failed", NULL);
		if (status != E_OK) {
			return status;
		}
		/* bind parameters */
        ret = SQLBindParameter(tps->stmtUpdate,
                               1,
                               SQL_PARAM_INPUT,
                               SQL_C_SLONG,
                               SQL_INTEGER,
                               0,
                               0,
                               &tps->totalTPSValue,
                               0,
                               NULL);
        status = checkError(ret, SQL_HANDLE_STMT, tps->stmtUpdate,
                            "TPS update",
                            "Bind parameter no. 1 in UPDATE TPS failed",
                            NULL);
        /* if solidDB load balancing, there are two more columns)*/
		if (solid_ha_stat) {
			ret = SQLBindParameter(tps->stmtUpdate,
                                   2,
                                   SQL_PARAM_INPUT,
                                   SQL_C_SLONG,
                                   SQL_INTEGER,
                                   0,
                                   0,
                                   &tps->db1,
                                   0,
                                   NULL);
			status = checkError(ret,
                                SQL_HANDLE_STMT, tps->stmtUpdate,
                                "TPS update",
                                "Bind parameter no. 2 in UPDATE TPS failed",
                                NULL);
			ret = SQLBindParameter(tps->stmtUpdate,
                                   3,
                                   SQL_PARAM_INPUT,
                                   SQL_C_SLONG,
                                   SQL_INTEGER,
                                   0,
                                   0,
                                   &tps->db2,
                                   0,
                                   NULL);
            status = checkError(ret,
                                SQL_HANDLE_STMT, tps->stmtUpdate,
                                "TPS update",
                                "Bind parameter no. 3 in UPDATE TPS failed",
                                NULL);
            ret = SQLBindParameter(tps->stmtUpdate,
                                   4,
                                   SQL_PARAM_INPUT,
                                   SQL_C_SLONG,
                                   SQL_INTEGER,
                                   0,
                                   0,
                                   &tps->clientID,
                                   0,
                                   NULL);
            status = checkError(ret, SQL_HANDLE_STMT, tps->stmtUpdate,
                                "TPS update",
                                "Bind parameter no. 3 in UPDATE TPS failed",
                                NULL);
        } else {
			ret = SQLBindParameter(tps->stmtUpdate,
                                   2,
                                   SQL_PARAM_INPUT,
                                   SQL_C_SLONG,
                                   SQL_INTEGER,
                                   0,
                                   0,
                                   &tps->clientID,
                                   0,
                                   NULL);
			status = checkError(ret, SQL_HANDLE_STMT, tps->stmtUpdate,
                                "TPS update",
                                "Bind parameter no. 2 in UPDATE TPS failed",
                                NULL);
        }
        return status;
}
#endif /* NO_TPS_TABLE */

#ifndef NO_TPS_TABLE

/*##**********************************************************************\
 *
 *      insertTPSrow
 *
 * Inserts a row to TPS table
 *
 * Parameters:
 *      hdbc 
 *          ODBC connection handle
 *
 *      clientID 
 *          Client identification (number)    
 *
 *      DBSchemaName
 *          Database schema name
 *
 * Return value:
 *      checkError/checkErrorS() return code
 */
int insertTPSrow(SQLHDBC hdbc, int clientID, char *DBSchemaName)
{
        SQLHSTMT    hstmt;

        char        insert[512];
        char        msg[512];
        SQLRETURN   rc;
        int         status;

        rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
        sprintf(insert, "INSERT INTO %stps VALUES (%d, 0, 0, 0)",
                (*DBSchemaName == '\0' ? "" : DBSchemaName),
                clientID);
        rc = SQLExecDirect(hstmt, (SQLCHAR *)insert, SQL_NTS);
        status = checkErrorS(
                rc,
                hstmt,
                NULL,
                "insertTPSrow()",
                NULL);
        rc = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_COMMIT);
        status = checkError(rc,
                            SQL_HANDLE_DBC,
                            hdbc,
                            NULL,
                            "insertTPSrow()",
                            NULL);
        rc = SQLFreeHandle(
                SQL_HANDLE_STMT,
                hstmt);
        if (status >= E_ERROR) {
            message_client('F',
                           "insertTPSrow()",
                           "Inserting the first row to TPS table failed");
        } else {
            sprintf(msg, "Inserted %d to TPS table.", clientID);
            message_client('D', "insertTPSrow", msg);
        }
        return status;
}
#endif /* NO_TPS_TABLE */

#ifndef NO_TPS_TABLE

/*#***********************************************************************\
 *
 *      deleteTPSrow
 *
 *  	Deletes a row from TPS table
 *
 * Parameters:
 *      connection
 *          ODBC connection handle
 *
 *      clientID 
 *          Client identification (number)
 *
 *      DBSchemaName 
 *          Database schema name
 *
 * Return value:
 *      checkError/checkErrorS() return code
 */
static int deleteTPSrow(connection_t* connection, int clientID,
                        char *DBSchemaName)
{
        SQLHSTMT    hstmt;
        SQLRETURN   rc;
        int         status;
        transaction_t transaction;
        char        delete[512];

        startTransaction(&transaction, connection);

        rc = SQLAllocHandle(SQL_HANDLE_STMT, connection->hdbc, &hstmt);

        status = checkError(rc,
                            SQL_HANDLE_DBC, connection->hdbc,
                            NULL,
                            "deleteTPSrow(): SQLAllocHandle",
                            NULL);
        
        if (status == 0) {
            SQLINTEGER boundClientID = clientID;
            rc = SQLBindParameter(hstmt,
                                  1, SQL_PARAM_INPUT,
                                  SQL_C_LONG, SQL_INTEGER, 0, 0,
                                  &boundClientID, 0, NULL);

            status = checkErrorS(
                    rc,
                    hstmt,
                    NULL,
                    "deleteTPSrow(): SQLBindParameter",
                    NULL);

            if (status == 0) {
                sprintf(delete, "DELETE FROM %stps WHERE id=?",
                        (*DBSchemaName == '\0' ? "" : DBSchemaName));

                rc = SQLExecDirect(hstmt, (SQLCHAR *)delete, SQL_NTS);

                if (rc == SQL_NO_DATA) {
                    rc = SQL_SUCCESS;
                }

                status = checkErrorS(rc,
                                     hstmt,
                                     NULL,
                                     "deleteTPSrow(): SQLExecDirect",
                                     NULL);
            }

            rc = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
            status = checkErrorS(rc,
                                 hstmt,
                                 NULL,
                                 "deleteTPSrow(): SQLFreeHandle",
                                 NULL);
        }

        status = endTransaction(&transaction,
                                status,
                                "TPS update",
                                "endTransaction()",
                                NULL,
                                NULL);
        
        return status;
}
#endif /* NO_TPS_TABLE */

#ifndef NO_TPS_TABLE

/*##**********************************************************************\
 *
 *      updateRealtimeStats
 *
 * Updates TPS values in the table.
 *
 * Parameters:
 *      client 
 *          pointer to client struct
 *
 *      throughput
 *          total throughput
 *
 *      db1_mqth
 *          throughput for primary DB
 *
 *      db2_
 *          throughput for secondary DB
 *
 * Return value:
 *      checkError() return value
 *
 */
int updateRealtimeStats(client_t* client, int throughput, int db1_mqth,
                        int db2_mqth)
{
        int status;
        SQLRETURN ret;
        transaction_t transaction;
        connection_t* connection = &client->connection;
        tpstable_t* tps = &connection->tps;

#ifdef _DEBUG
        char txt_buf[512]; /* AUTOBUF */        
        if (client->solid_ha_stat) {
            sprintf(txt_buf, "Throughput: %d tps (%d, %d)", throughput,
                    db1_mqth,
                    db2_mqth);
        } else {
            sprintf(txt_buf, "Throughput: %d tps", throughput);
        }
        message_client('I', "", txt_buf);
#endif

        startTransaction(&transaction, connection);
        /* Since ODBC does not allow executing several transactions
         * from the single connection (HDBC), care should be taken
         * to avoid interference of test and TPS transactions between
         * each other. Luckily, currently TPS is updated after test
         * transaction has completed so there is no problem now,
         * however, if that should change, please pay attention.
         * Also, autocommit mode between test transactions and TPS
         * transaction must be the same.
         */

        /* update the TPS table */
        tps->clientID = client->clientID;
        tps->totalTPSValue = throughput;

        if (client->solid_ha_stat) {
            tps->db1=db1_mqth;
            tps->db2=db2_mqth;
        }

        ret = SQLExecute(tps->stmtUpdate);
        
        status = checkError(ret,
                            SQL_HANDLE_STMT, tps->stmtUpdate,
                            "TPS update",
                            "UPDATE TPS table failed",
                            &transaction);
        
        status = endTransaction(&transaction,
                                status,
                                "TPS update",
                                "endTransaction()",
                                NULL,
                                client);
                
        return status;
}
#endif
