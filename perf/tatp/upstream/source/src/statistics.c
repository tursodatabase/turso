/**********************************************************************\
 **  source       * statistics.c
 **  description  * Collects transaction statistics from
 **	               the clients and saves them either to TIRDB or
 **		           result files
 **
 **
 **  Copyright IBM Corporation 2004, 2011.
 **
 **  This program is made available under the terms of the Common Public
 **  License 1.0 as published by the Open Source Initiative (OSI).
 **  http://www.opensource.org/licenses/cpl1.0.php
 **
\**********************************************************************/

#include "statistics.h"
#ifdef WIN32
#include <windows.h>
#else
#include <sys/time.h>
#endif
#include <math.h>

static communication_t g_comm;
static log_t g_log;

/*##**********************************************************************\
 *
 *      getLogObject
 *
 * Returns log object.
 *
 * Parameters:
 *      none
 * Return value 
 *      pointer to log object
 */
log_t* getLogObject( )
{
        return &g_log;
}

/*##**********************************************************************\
 *
 *      init_state
 *
 * Initializes global variables.
 *
 * Parameters:
 *      none
 * Return value:
 *      0 - always
 */
int init_state( )
{
        int i;        
#ifdef _DEBUG
        /* For TATP software performance analysis -> not needed
           in actual benchmark runs */
        initializeTiming();
#endif

        for (i = 0; i < MAX_CLIENTS; i++) {
            /* intialize the structure holding the client
               information */
            state.client[i].id = 0;
            state.client[i].state = NOT_LOGGED_IN;
        }

        /* Few variable initializations */
        /* mqth_time_slot = 0; */
        clients_online = 0;
        clientErrorCount = 0;
        statisticErrorCount = 0;
        dbErrorCount = 0;
        state.phase = PARAMETERS;
        state.end = 0;
        return 0;
}

/*##**********************************************************************\
 *
 *      handle_parameters
 *
 * Reads the command line parameters passed by the
 * Control module. The command line is of the form:
 *	statistics
 *	<1>	 <test_id>
 *	<2>	 <rampup time (in seconds)>
 *	<3>	 <control module IP address>
 *	<4>	 <TIRDB connect string>
 *  <5>  <result file name>
 *	<6>  <throughput_resolution>
 *	<7>	 <logging verbosity>
 *	<8>	 <transaction_type1>
 *	<9> <transaction_type2>
 *	...
 *	<transaction_typen>
 *
 * Parameters:
 *      argc
 *			number of parameters
 *
 *      argv
 *          the actual command parameters
 *
 * Return value:
 *      0  - success
 *	   -1 - error
 */
int handle_parameters(int argc, char *argv[])
{
        char message_text[MESSAGE_SIZE];
        int verbose_level;
        int i, j;

        /* Check that Control did not pass too many transaction
           types to Statistics */
        if (argc > (MAX_TRANSACTION_TYPES + STATISTICS_STATIC_ARGC - 1) ) {
            logRecord(FATAL, "handle_parameters",
                      "Got too many transaction types from Control" );
            state.phase = FINAL;
            return -1;
        }

        if (argc < (STATISTICS_STATIC_ARGC - 1)) {
            /* This should not actually happen */
            printf("Statistics error: Wrong number of arguments...exiting.\n");
            state.phase = FINAL;
            return -1;
        }

        testRunId = atoi(argv[1]);
        rampup_time = atoi(argv[2]);

        /* Assign control module IP address to a variable. The
           address is passed to Statistics as a command parameter.
           (not needed for now) */	

        if (strncmp(argv[4], "NULL", 4) == 0) {
            *tirdbConnectString = '\0';
        } else {
            strncpy(tirdbConnectString, argv[4], CONNECT_STRING_LENGTH);
        }
        if (strncmp(argv[5], "NULL", 4) == 0) {
            *resultFileName = '\0';
        } else {
            strncpy(resultFileName, argv[5], W_L);
        }
        
        throughput_resolution = atoi(argv[6]);
        verbose_level = atoi(argv[7]);

        /* Store transaction names in a structure to enable
           transaction name->number mapping */
        j = 0;
        for (i = STATISTICS_STATIC_ARGC; i < argc; i++) {
            strncpy(transaction_names[j], argv[i], TRANSACTIONTYPE_SIZE);
            j++;
        }

        /* Log file initialization */
        initializeLog(verbose_level, "STATISTICS", 3);
        createLog(STATISTICS_LOG_FILE_NAME);

        /* A log message indicating the start of Statistics */
        sprintf(message_text, "Started");
        logRecord(INFO, "handle_parameters", message_text);

        /* Write out all arguments (only if DEBUG level verbosity
           is selected) */
        for (i = 0; i < argc; i++) {
            sprintf(message_text, "argv[%d] = %s", i, argv[i]);
            logRecord(DEBUG, "handle_parameters", message_text);
        }

        /* state change */
        state.phase = INIT_TRANS;
        return 0;
}

/*##**********************************************************************\
 *
 *      init_trans
 *
 * Initializes the transaction related data structures.
 * The memory for response time structures is statically
 * reserved but MQTh structures use dynamic memory allocation
 * because the size of the memory needed depends on a
 * variable (throughput_resolution) defined in the TDF file
 *
 * Parameters:
 *      none
 * Return value:
 *      0 - success
 *	   -1 - error
 */
int init_trans( )
{
        int i, j;

        /* Reserve memory for MQTH values.
           Note: data is stored the following way in MQTH structure
           t1 s1 t1 s2 t1 s3 ... t2 s1 t2 s2 t2 s3 ... t3 s1 t3 s2 t3 s3 ...
           where t='transaction type' and s='time slot number' */
        num_of_time_slots = (MAX_TEST_LENGTH / throughput_resolution) + 1;
        transactions.mqth =
            (int*) malloc(sizeof(int) * num_of_time_slots);
        if (!transactions.mqth) {
            /* Dynamic memory allocation failed */
            logRecord(FATAL, "init_trans",
                      "Could not reserve memory for the MQTH structure");
            state.phase = FINAL;
            return -1;
        }

        for (j = 0; j < num_of_time_slots; j++) {
            /* intialize the structure holding the mqth values */
            transactions.mqth[j] = 0;
        }
        for (i = 0; i < MAX_TRANSACTION_TYPES; i++) {
            for (j = 0; j < MAX_RESP_TIME_SLOTS; j++) {
                /* intialize the structure holding the response
                   time values */
                transactions.resp[i][j] = 0;
            }

        }
        for (i = 0; i < MAX_NUM_OF_DB_ERRORS; i++) {
            /* intialize the error message structure */
            /* Note: in current version of TATP we are not reporting
               target db errors (the code is for future needs) */
            *(transactions.dbErrors[i].errorCode) = 0;
        }

        /* state change */
        state.phase = INIT_COMM;
        return 0;
}

/*##**********************************************************************\
 *
 *      init_communications
 *
 * Initialized the communication ports for listening Clients and sending to
 * Control. Also checks that TIRDB connections can be established (TIRDB
 * is actually used only in the end of the life cycle of Statistics).
 * Finally, sends MSG_OK to Control.
 *
 * Parameters:
 *      none
 * Return value:
 *      0 - success
 *     !0 - error
 */
int init_communications( )
{
        int retval;
        char message_text[MESSAGE_SIZE];
        SQLHENV tirdb_env;
        SQLHDBC tirdb;
        struct message_dataS dummy_data;

        if ((retval = initializeCommunicationGlobal()) != 0) {
            sprintf(message_text,
                    "Cannot initialize communication module, retval: %d",
                    retval);
            logRecord(FATAL, "init_communications", message_text);
            state.phase = FINAL;
            return -1;
        }
        if ((retval = initializeCommunication(&g_comm, "STATISTICS")) != 0) {
            sprintf(message_text,
                    "Cannot initialize communication module, retval: %d",
                    retval);
            logRecord(FATAL, "init_communications", message_text);
            state.phase = FINAL;
            return -1;
        }

        if ((retval = createListener(&g_comm, STATISTICS_PORT)) != 0) {
            sprintf(message_text, "Cannot create listener, retval: %d",
                    retval);
            logRecord(FATAL, "init_communications", message_text);
            state.phase = FINAL;
            return -1;
        }

        if ((state.control_socket = createConnection("127.0.0.1",
                                                     MAIN_CONTROL_PORT)) == 0) {
            sprintf(message_text,
                    "Cannot create connection to control, retval: %d",
                    retval);
            logRecord(FATAL, "init_communications", message_text);
            state.phase = FINAL;
            return -1;
        }

        if ((retval = initializeMessaging()) != 0) {
            sprintf(message_text,
                    "Cannot initialize Messaging, retval: %d", retval);
            logRecord(FATAL, "init_communications", message_text);
            state.phase = FINAL;
            return -1;
        }

        /* Check the TIRDB connection */
        if (*tirdbConnectString != '\0') {
            if (ConnectDB(&tirdb_env, &tirdb, tirdbConnectString,
                          "TIRDB")) {
                /* Could not connect */
                if (*resultFileName == '\0') {
                    logRecord(FATAL, "init_communications",
                              "Cannot connect to TIRDB or use result file");
                    state.phase = FINAL;
                    return -1;
                } else {
                    /* fallback to result file */
                    logRecord(ERROR_, "init_communications",
                              "Cannot connect to TIRDB ... using result file");
                    storeResults = MODE_TO_SQLFILE;
                }
            } else {
                /* TIRDB ok */
                storeResults = MODE_TO_TIRDB;
                DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
            }
        } else if (*resultFileName != '\0') {
            storeResults = MODE_TO_SQLFILE;
        }
        
        if (storeResults == MODE_TO_SQLFILE) {
            openFileForWrite(&fResults, resultFileName);
            if (fResults == NULL) {
                logRecord(FATAL, "init_communications",
                          "Cannot create result file");
                state.phase = FINAL;
                return -1;                    
            }                
        }

        if (sendDataS(state.control_socket, STATISTICS_ID, MSG_OK,
                      &dummy_data)) {
            logRecord(FATAL, "init_communications",
                      "Cannot send MSG_OK to Control");
            state.phase = FINAL;
            return -1;
        }

        /* state change */
        state.phase = MESSAGES;
        return 0;
}

/*##**********************************************************************\
 *
 *      handle_message
 *
 * Get a message from the port statistics is listening
 * to and handle the message according to its type (MSG_REG,
 * MSG_MQTH, MSG_RESPTIME or MSG_LOGOUT). If multiple
 * messages are pending, handle at most MESSAGE_LOOP_ITERATIONS
 * of them before returning.
 * <function description>
 *
 * Parameters:
 *      none
 * Return value:
 *      0 - success
 *     -1 - error
 */
int handle_message( )
{
        int retval, i;
        char message_text[MESSAGE_SIZE];
        /* buf is for receiving data from the socket */
        char buf[MAX_MESSAGE_SIZE];

        /* Sender ID of the received message */
        int senderId;
        /* Type of the received message. */
        int messageType;
        /* Struct to hold the received data */
        struct message_dataS data;

        /* To enter the while loop */
        retval = 1;
        i = 0;
        while ((retval > 0) && (i < MESSAGE_LOOP_ITERATIONS)) {
            /* We iterate in the message retrieval loop at most
               MESSAGE_LOOP_ITERATIONS times (if receiveMessage
               returns no message also then the loop is exited).
               This was proofed to work well in practise. */
            i++;
            retval = receiveMessage(&g_comm, buf);

            if (retval < 0) {
                /* We had a problem receiving data from a client */
                sprintf(message_text,
                        "Problems receiving data, retval: %d", retval);
                logRecord(FATAL, "handle_message", message_text);
                state.phase = END_COMM;
                return -1;
            }
            else if (retval > 0) {
                /* A message was received. Decode it. */
                retval = decodeMessage(buf, &senderId,
                                       &messageType, &data);
                if (retval < 0) {
                    /* A protocol error, should not happen */
                    sprintf(message_text,
                            "Problems with decoding data, retval: %d",
                            retval);
                    logRecord(FATAL, "handle_message", message_text);
                    state.phase = END_COMM;
                    return -1;
                }
                retval = 1; /* To stay in the while loop */
                switch (messageType) {
                    case MSG_MQTH:
                        if (handle_mqthMsg(senderId, &data) != 0) {
                            return -1;
                        }
                        break;
                    case MSG_RESPTIME:
                        if (handle_resptimeMsg(senderId, &data) != 0) {
                            return -1;
                        }
                        break;
                    case MSG_REG:
                        if (handle_registration(senderId, &data) != 0) {
                            return -1;
                        }
                        break;
                    case MSG_LOGOUT:
                        if (handle_logout(senderId, &data) != 0) {
                            return -1;
                        }
                        break;
                    default:
                        sprintf(message_text,
                                "Unknown messagetype: %d", messageType);
                        logRecord(WARNING, "handle_message",
                                  message_text);
                        state.phase = END_COMM;
                        return -1;
                        break;
                }
            }
        } /* while retval > 0 */

        /* No messages were received -> take a nap */
        if (retval == 0) SLEEP(STATISTICS_IDLE);

        return 0;
}

/*##**********************************************************************\
 *
 *      handle_registration
 *
 * Handles the registration messages sent by Clients.
 * In practise stores the client data to the data
 * structure reserved for logged clients.
 *
 * Parameters:
 *      sender_id
 *			The client idnetification
 *
 *      data
 *			The data sent by the client
 *
 * Return value:
 *      0  - success
 *     -1 - error
 */
int handle_registration(int sender_id, struct message_dataS *data)
{
        int i;
        char message_text[MESSAGE_SIZE];

        /* Get the registration time at first */
#ifdef WIN32
        struct _timeb registration_time;
        _ftime(&registration_time);
#else
        struct timeval registration_time;
        gettimeofday(&registration_time, NULL);
#endif

        /* The first client registration starts the test (or rampup)
           -> the timer is started */
        if (clients_online == 0) {
            latest_mqth_start_time = registration_time;

            if (rampup_time > 0) {
                /* Rampup time is defined for the benchmark */
                rampUpTimeOn = 1;
                rampup_start_time = registration_time;

                sprintf(message_text,
                        "Rampup time of %d minutes",
                        rampup_time);
                logRecord(INFO, "handle_registration",
                          message_text);
            }
            else {
                /* No rampup time defined */
                rampUpTimeOn = 0;
                test_start_time = registration_time;
            }
        }

        /* We have a new client logged in */
        clients_online++;

        /* Check the test_run_id */
        if (testRunId != data->sdata.reg.testID) {
            /* Something wrong <- test run identifier differs from
               the one Statistics have */
            sprintf(message_text, "Client%d: tesID=%d, Statistics: testID=%d",
                    sender_id, data->sdata.reg.testID, testRunId);
            logRecord(ERROR_, "handle_registration", message_text);
        }

        i = 0;
        while (state.client[i].state == LOGGED_IN) {
            /* Search for the first free slot from the data structure
               holding the client information */
            i++;
            if (i == MAX_CLIENTS) {
                /* Too many clients logged in */
                logRecord(FATAL, "Client logging", "Too many clients");
                state.phase = END_COMM;
                return -1;
            }
        }
        /* Reserve the slot for the client and fill up the information */
        state.client[i].id = sender_id;
        state.client[i].state = LOGGED_IN;
        sprintf(message_text, "Client %d logged in", sender_id);
        logRecord(DEBUG, "handle_registration", message_text);

        return 0;
}

/*##**********************************************************************\
 *
 *      handle_logout
 *
 * Handles the logout messages sent by Clients. Removes
 * the client information from the client data structures
 * and in case of the last client logging off checks
 * the client error count (number of errors encountered
 * by the clients) and acts accordingly (no benchmark
 * result printing if any errors are found).
 *
 * Parameters:
 *      sender_id
 *          The client idnetification
 *
 *      data
 *          The data sent by the client
 *
 * Return value:
 *      0  - success
 *     -1 - error
 */
int handle_logout(int sender_id, struct message_dataS *data)
{
        int i = 0;
        char message_text[MESSAGE_SIZE];

        while (state.client[i].id != sender_id) {
            /* search for the client from the structure */
            i++;
            if (i == MAX_CLIENTS) {
                /* Internal error: Existing client was not found
                   from the structure (should not come to this) */
                logRecord(FATAL, "Client logout", "Client not found");
                state.phase = END_COMM;
                return -1;
            }
        }
        /* Remove the client information */
        state.client[i].id = 0;
        state.client[i].state = NOT_LOGGED_IN;
        clients_online--;

        if (data->sdata.reg.data > 0) {
            /* Client stopped abnormally -> Increase the client
               error counter */
            clientErrorCount += data->sdata.reg.data;
        }

        /* state change in case of a logout message from the
           last active client */
        for (i = 0; i < MAX_CLIENTS; i++) {
            if (state.client[i].state == LOGGED_IN) {
                /* A client exists */
                break;
            }
        }
        if (i >= MAX_CLIENTS) {
            /* No clients logged in any more */
            /* Check the client error count */
            logRecord(INFO, "handle_logout", "All clients finished");
            if (clientErrorCount > 0) {
                logRecord(FATAL, "handle logout",
                          "Client(s) stopped abnormally -> BENCHMARK RESULTS "
                          "NOT STORED");
                state.phase = END_COMM;
            }
            else {
                /* No errors -> move to the 'print out the results' mode */
                state.phase = OUTPUT;
            }
        }
        sprintf(message_text, "Client %d logged out", sender_id);
        logRecord(DEBUG, "handle_logout", message_text);
        return 0;
}

/*##**********************************************************************\
 *
 *      handle_mqthMsg
 *
 * Handles the MQTH messages sent by Clients.
 * Each message contains a time slot and the number of
 * successful transactions executed during that time slot
 *
 * Parameters:
 *      sender_id 
 *          The client idnetification
 *
 *      data
 *			The data sent by the client
 *
 * Return value:
 *      0 - success
 *	   -1 - error
 */
int handle_mqthMsg(int sender_id, struct message_dataS *data)
{
        /* Is the data received actually from the time of rampup */

        if (throughput_resolution * data->sdata.mqth.timeSlotNum
            >= rampup_time * 60) {
			/* The time slot hits the test time (not rampup time) */
            transactions.mqth[data->sdata.mqth.timeSlotNum] +=
                data->sdata.mqth.transCount;
        }

        /* Store the time slot */
        if (last_used_time_slot < data->sdata.mqth.timeSlotNum) {
            last_used_time_slot = data->sdata.mqth.timeSlotNum;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      handle_resptimeMsg
 *
 * Handles the response time messages sent by Clients. Each message
 * contains a transaction type and response time in milliseconds and the
 * transaction count for the combination of the two abovementioned.
 *
 * Parameters:
 *      sender_id 
 *          The client identification
 *
 *      data 
 *          The data sent by the client
 *
 * Return value:
 *      0 - always
 */
int handle_resptimeMsg(int sender_id, struct message_dataS *data)
{
        int i;
        i = 0;
        while (strncmp(transaction_names[i],
                       data->sdata.resptime.transactionType,
                       TRANSACTIONTYPE_SIZE) != 0) {
			i++;
			if (i == MAX_TRANSACTION_TYPES) {
				/* Transaction name was not found from the data structure ?
                   (should not come to this)*/
				logRecord(FATAL, "Client logging",
                          "Transaction type not found");
				state.phase = END_COMM;
				return -1;
			}
        }
#ifndef LINEAR_RESPONSE_SCALE

        if (transactions.resp_bounds[i][data->sdata.resptime.slot] == 0) {
            transactions.resp_bounds[i][data->sdata.resptime.slot] =
                data->sdata.resptime.responseTimeBound;
        } else {
            if (transactions.resp_bounds[i][data->sdata.resptime.slot]
                != data->sdata.resptime.responseTimeBound) {
                logRecord(ERROR_, "Client logging",
                          "Response time slots mismatch");
            }
        }
        transactions.resp[i][data->sdata.resptime.slot] +=
            data->sdata.resptime.transactionCount;
#else
        transactions.resp[i][data->sdata.resptime.responseTime] +=
            data->sdata.resptime.transactionCount;
#endif
        return 0;
}

/*##**********************************************************************\
 *
 *      count_and_store_results
 *
 * Counts the average MQTh value.
 * If TIRDB is used also stores MQTh values and response time
 * figures to TIRDB. Also the benchmark status flag in TIRDBs
 * TEST_RUNS table is updated (the column TEST_COMPLETED).
 *
 * Parameters:
 *      none
 *
 * Return value:
 *      0  - success
 *     -1 - error
 */
int count_and_store_results( )
{
        int i, j;
        /* Used to sum up all the MQTh values from the MQTh
           time slots */
        int summed_time_slot;
        SQLHENV tirdb_env;
        SQLHDBC tirdb;
        RETCODE rc;
        /* The statement handles to different SQL statements */
        SQLHSTMT resp_time_stmt, mqth_stmt, completed_stmt;
#ifndef LINEAR_RESPONSE_SCALE
        const char* response_sql = RESP_TIME_SCALE_INSERT;
        SQLHSTMT resp_time_percentile_stmt;
        SQLINTEGER slot_no;
        SQLINTEGER bound;
#else
        const char* response_sql = RESP_TIME_INSERT;
#endif
        SQLUSMALLINT paramNumber;
        /* Variables used in parameter bindings with the
           prepared database statements */
        char transactionName[TRANSACTIONTYPE_SIZE];
#ifdef LINEAR_RESPONSE_SCALE
        int responseTime;
#endif
        int numOfHits, time_slot_num, mqth;
        int stored_time_slot_num;
        char message_text[MESSAGE_SIZE];

#ifndef LINEAR_RESPONSE_SCALE
        int resp_time;
        int responsetime_percentile[MAX_TRANSACTION_TYPES];

        logRecord(INFO, "store_results",
                  "90% response time (us), by transaction");

        /* compute 90%th percentile */
        for (i = 0; i != MAX_TRANSACTION_TYPES; i++) {
            int transactionCount = 0;

            responsetime_percentile[i] = 0;

            for (j = 0; j < MAX_RESP_TIME_SLOTS; j++)
                transactionCount += transactions.resp[i][j];

            if (transactionCount != 0) {

                int percentileCount;
                int cumulative_hits;
                int responsePercentile;
                int lower_bound;
                int upper_bound;
                int lower_hits;
                int upper_hits;

                percentileCount = (int)(0.9 * transactionCount + 0.5);

                cumulative_hits = 0;
                for (j = 0; j != MAX_RESP_TIME_SLOTS; j++) {
                    if (percentileCount < cumulative_hits
                        + transactions.resp[i][j])
                        break;
                    cumulative_hits += transactions.resp[i][j];
                }

                lower_bound = (j != 0) ? transactions.resp_bounds[i][j - 1] : 0;
                upper_bound = transactions.resp_bounds[i][j];
                lower_hits = cumulative_hits;
                upper_hits = cumulative_hits + transactions.resp[i][j];

                responsePercentile = lower_bound
                    + (int)floor((double) (upper_bound - lower_bound)
                                 / (upper_hits - lower_hits)
                                 * (percentileCount - lower_hits) + 0.5);

                responsetime_percentile[i] = responsePercentile;

                sprintf(message_text, "%s: %d",
                        transaction_names[i],
                        responsePercentile);

                logRecord(INFO, "store_results", message_text);

                sprintf(message_text, "total %d, 90%% %d, slot %d, "
                        "lower_bdry %d, upper_bdry %d, lower_hits %d, "
                        "upper_hits %d",
                        transactionCount,
                        (int) floor(percentileCount + 0.5),
                        j,
                        lower_bound,
                        upper_bound,
                        lower_hits,
                        upper_hits);
            
                logRecord(DEBUG, "store_results", message_text);
            }
        }

#endif

#ifndef LINEAR_RESPONSE_SCALE
        resp_time_percentile_stmt = 0;
#endif

        if (storeResults == MODE_TO_TIRDB) {
            /* TIRDB is used */
            logRecord(INFO, "write_results", "Write results to TIRDB");
            
            if (ConnectDB(&tirdb_env, &tirdb, tirdbConnectString, "TIRDB")) {
                logRecord(FATAL, "write_results", "Cannot connect to TIRDB");
                state.phase = END_COMM;
                return -1;
            }
            
            resp_time_stmt = 0;
            mqth_stmt = 0;
            completed_stmt = 0;
            
            /* Allocate statement handles */
            for (i = 0; i < 4; i++) {
                switch (i) {
                    case 0:
                        rc = SQLAllocHandle(SQL_HANDLE_STMT, tirdb,
                                            &resp_time_stmt);
                        break;
                    case 1:
                        rc = SQLAllocHandle(SQL_HANDLE_STMT, tirdb,
                                            &mqth_stmt);
                        break;
                    case 2:
                        rc = SQLAllocHandle(SQL_HANDLE_STMT, tirdb,
                                            &completed_stmt);
                        break;
                    case 3:
#ifndef LINEAR_RESPONSE_SCALE
                        rc = SQLAllocHandle(SQL_HANDLE_STMT, tirdb,
                                            &resp_time_percentile_stmt);
#endif
                        break;
                }
                if (error_c(tirdb, rc)) {
                    logRecord(FATAL, "write_results", "SQLAllocHandle failed");
                    state.phase = END_COMM;
                    return -1;
                }
            }
        }
        
        if (storeResults == MODE_TO_TIRDB) {

            /* Prepare statements */
            rc = SQLPrepare(resp_time_stmt, CHAR2SQL(response_sql),
                            SQL_NTS);
            if (error_s(resp_time_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot prepare the RESPONSE_TIME statement "
                          "for TIRDB");
                state.phase = END_COMM;
                return -1;
            }
            rc = SQLPrepare(mqth_stmt, CHAR2SQL(MQTH_INSERT), SQL_NTS);
            if (error_s(mqth_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot prepare the MQTH statement for TIRDB");
                state.phase = END_COMM;
                return -1;
            }
            rc = SQLPrepare(completed_stmt, CHAR2SQL(COMPLETED_UPDATE),
                            SQL_NTS);
            if (error_s(completed_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot prepare the COMPLETED statement for "
                          "TIRDB");
                state.phase = END_COMM;
                return -1;
            }
#ifndef LINEAR_RESPONSE_SCALE
            rc = SQLPrepare(resp_time_percentile_stmt,
                            CHAR2SQL(RESP_TIME_PERCENTILE_INSERT), SQL_NTS);
            if (error_s(resp_time_percentile_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot prepare the COMPLETED statement for "
                          "TIRDB");
                state.phase = END_COMM;
                return -1;
            }
#endif
            
            /* Bind parameters for response time statement */
            paramNumber = 1;
            rc = SQLBindParameter(resp_time_stmt, paramNumber++,
                                  SQL_PARAM_INPUT,
                                  SQL_C_SLONG, SQL_INTEGER, 0, 0,
                                  &testRunId, 0, NULL);
            if (error_s(resp_time_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot bind a parameter in SQL");
                state.phase = END_COMM;
                return -1;
            }
            rc = SQLBindParameter(resp_time_stmt, paramNumber++,
                                  SQL_PARAM_INPUT,
                                  SQL_C_CHAR, SQL_CHAR,
                                  TIRDB_TRANSACTION_NAME_LEN, 0,
                                  transactionName, 0, NULL);
            if (error_s(resp_time_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot bind a parameter in SQL");
                state.phase = END_COMM;
                return -1;
            }
#ifndef LINEAR_RESPONSE_SCALE
            rc = SQLBindParameter(resp_time_stmt, paramNumber++,
                                  SQL_PARAM_INPUT,
                                  SQL_C_SLONG, SQL_INTEGER, 0, 0,
                                  &slot_no, 0, NULL);
            if (error_s(resp_time_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot bind a parameter in SQL");
                state.phase = END_COMM;
                return -1;
            }
            rc = SQLBindParameter(resp_time_stmt, paramNumber++,
                                  SQL_PARAM_INPUT,
                                  SQL_C_SLONG, SQL_INTEGER, 0, 0,
                                  &bound, 0, NULL);
            if (error_s(resp_time_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot bind a parameter in SQL");
                state.phase = END_COMM;
                return -1;
            }
#else
            rc = SQLBindParameter(resp_time_stmt, paramNumber++,
                                  SQL_PARAM_INPUT,
                                  SQL_C_SLONG, SQL_INTEGER, 0, 0,
                                  &responseTime, 0, NULL);
            if (error_s(resp_time_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot bind a parameter in SQL");
                state.phase = END_COMM;
                return -1;
            }
#endif
            rc = SQLBindParameter(resp_time_stmt, paramNumber++,
                                  SQL_PARAM_INPUT,
                                  SQL_C_SLONG, SQL_INTEGER, 0, 0,
                                  &numOfHits, 0, NULL);
            if (error_s(resp_time_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot bind a parameter in SQL");
                state.phase = END_COMM;
                return -1;
            }
            
            /* Bind parameters for mqth statement */
            paramNumber = 1;
            rc = SQLBindParameter(mqth_stmt, paramNumber++,
                                  SQL_PARAM_INPUT,
                                  SQL_C_SLONG, SQL_INTEGER, 0, 0,
                                  &testRunId, 0, NULL);
            if (error_s(resp_time_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot bind a parameter in SQL");
                state.phase = END_COMM;
                return -1;
            }
            rc = SQLBindParameter(mqth_stmt, paramNumber++,
                                  SQL_PARAM_INPUT,
                                  SQL_C_SLONG, SQL_INTEGER, 0, 0,
                                  &stored_time_slot_num, 0, NULL);
            if (error_s(resp_time_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot bind a parameter in SQL");
                state.phase = END_COMM;
                return -1;
            }
            rc = SQLBindParameter(mqth_stmt, paramNumber++,
                                      SQL_PARAM_INPUT,
                                      SQL_C_SLONG, SQL_INTEGER, 0, 0,
                                      &mqth, 0, NULL);
            if (error_s(resp_time_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot bind a parameter in SQL");
                state.phase = END_COMM;
                return -1;
            }
            
            /* Bind parameters for completed_test statement */
            paramNumber = 1;
            rc = SQLBindParameter(completed_stmt, paramNumber++,
                                  SQL_PARAM_INPUT,
                                  SQL_C_SLONG, SQL_INTEGER, 0, 0,
                                  &testRunId, 0, NULL);
            if (error_s(completed_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot bind a parameter in SQL");
                state.phase = END_COMM;
                return -1;
            }

#ifndef LINEAR_RESPONSE_SCALE
            /* Bind parameters for resp_time_percentile statement */
            paramNumber = 1;
            rc = SQLBindParameter(resp_time_percentile_stmt,
                                  paramNumber++,
                                  SQL_PARAM_INPUT,
                                  SQL_C_SLONG, SQL_INTEGER, 0, 0,
                                  &testRunId, 0, NULL);
            if (error_s(resp_time_percentile_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot bind a parameter in SQL");
                state.phase = END_COMM;
                return -1;
            }
            rc = SQLBindParameter(resp_time_percentile_stmt,
                                  paramNumber++,
                                  SQL_PARAM_INPUT,
                                  SQL_C_CHAR, SQL_CHAR,
                                  TIRDB_TRANSACTION_NAME_LEN,
                                  0, transactionName, 0, NULL);
            if (error_s(resp_time_percentile_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot bind a parameter in SQL");
                state.phase = END_COMM;
                return -1;
            }
            rc = SQLBindParameter(resp_time_percentile_stmt,
                                  paramNumber++,
                                  SQL_PARAM_INPUT,
                                  SQL_C_SLONG, SQL_INTEGER, 0, 0,
                                  &resp_time, 0, NULL);
            if (error_s(resp_time_percentile_stmt, rc, NULL)) {
                logRecord(FATAL, "write_results",
                          "Cannot bind a parameter in SQL");
                state.phase = END_COMM;
                return -1;
            }
            
        } /* if (storeResults == MODE_TO_TIRDB) */

#endif

        /* Store the response time figures */
        /* Iterate through transaction types */
        for (i = 0; i < MAX_TRANSACTION_TYPES; i++) {
            
            strncpy(transactionName, transaction_names[i],
                    TRANSACTIONTYPE_SIZE);
            
#ifndef LINEAR_RESPONSE_SCALE

            /* add more precise responsetime */
            if (responsetime_percentile[i] > 0) {
                resp_time = responsetime_percentile[i];
                
                if (storeResults == MODE_TO_SQLFILE) {
                    fwrite(RESP_TIME_PERCENTILE_INSERT, sizeof(char),
                           strchr(RESP_TIME_PERCENTILE_INSERT, '?')
                           - RESP_TIME_PERCENTILE_INSERT, fResults);
                    fprintf(fResults, "?, '%s', %d)", transactionName, resp_time);
                    fputs(";\n", fResults);
                    fflush(fResults);
                } else if (storeResults == MODE_TO_TIRDB) {
                    rc = SQLExecute(resp_time_percentile_stmt);
                    if (error_s(resp_time_percentile_stmt, rc, NULL)) {
                        /* Something went wrong */
                        logRecord(FATAL, "write_results",
                                  "SQLExecute (on RESULT_RESPONSE) "
                                  "failed -> partial data in TIRDB");
                        state.phase = END_COMM;
                        return -1;                           
                    }
                }
            }
#endif

            /* Iterate through all the response time figures */
            for (j = 0; j < MAX_RESP_TIME_SLOTS; j++) {
                
#ifndef LINEAR_RESPONSE_SCALE
                if (transactions.resp_bounds[i][j] != 0) 
#else
                if (transactions.resp[i][j] != 0)                    
#endif
                {
                    /* There were hits to the response time 'j'
                       -> store it to TIRDB */
#ifndef LINEAR_RESPONSE_SCALE
                    slot_no = j;
                    bound = transactions.resp_bounds[i][j];
#else
                    responseTime = transactions.resp_bounds[i][j];
#endif
                    numOfHits = transactions.resp[i][j];
                    
                    if (storeResults == MODE_TO_SQLFILE) {
                        
                        fwrite(response_sql, sizeof(char),
                               strchr(response_sql, '?') - response_sql,
                               fResults);
                        fprintf(fResults, "?, '%s', ", transactionName);
#ifndef LINEAR_RESPONSE_SCALE
                        fprintf(fResults, "%d, %d, ", slot_no, bound);
#else
                        fprintf(fResults, "%d, ", responseTime);
#endif
                        fprintf(fResults, "%d)", numOfHits);
                        fputs(";\n", fResults);
                        fflush(fResults);
                    } else if (storeResults == MODE_TO_TIRDB) {
                        rc = SQLExecute(resp_time_stmt);
                        if (error_s(resp_time_stmt, rc, NULL)) {
                            /* Something went wrong */
                            logRecord(FATAL, "write_results",
                                      "SQLExecute (on RESULT_RESPONSE) "
                                      "failed -> "
                                      "partial data in TIRDB");
                            state.phase = END_COMM;
                            return -1;
                        }
                    }
                }
            } /* for (j = 0 ... MAX_RESP_TIME_SLOTS */
        } /* for (i = 0 ... MAX_TRANSACTION_TYPES */
 
        /* Count and store the MQTh values */
        /* Sum the values over the transaction types because
           (at least in this version) we don't make a difference
           between the trans types (for mqth values, that is).
           It might be interesting to see the throughput per
           tranasaction type though ... */
            
        summed_overall_mqth = 0;
        stored_time_slot_num = 0;
        for (time_slot_num = rampup_time*60 / throughput_resolution;
             time_slot_num <= last_used_time_slot; time_slot_num++) {
                
            summed_time_slot = 0;
            /* it does not matter if we sum non-used slots too */
            summed_time_slot += transactions.mqth[time_slot_num];

            summed_time_slot /= throughput_resolution;

            /* TIRDB is used; store the mqth of a time slot to TIRDB */
            mqth = summed_time_slot;
                
            if (storeResults == MODE_TO_SQLFILE) {                    
                fwrite(MQTH_INSERT, sizeof(char),
                       strchr(MQTH_INSERT, '?') - MQTH_INSERT, fResults);
                fprintf(fResults, "?, %d, %d)", stored_time_slot_num, mqth);
                fputs(";\n", fResults);
                fflush(fResults);
            } else if (storeResults == MODE_TO_TIRDB) {
                rc = SQLExecute(mqth_stmt);
                    
                if (error_s(mqth_stmt, rc, NULL)) {
                    /* Something went wrong */
                    logRecord(FATAL, "write_results",
                              "SQLExecute (on RESULT_THROUGHPUT) "
                              "failed -> "
                              "partial data in TIRDB");
                    state.phase = END_COMM;
                    return -1;                        
                }
            }
            stored_time_slot_num  += 1;                

            summed_overall_mqth += summed_time_slot;
        }

        if (storeResults == MODE_TO_TIRDB) {
            /* TIRDB is used */
            /* Update the TEST_RUNS.TEST_COMPLETED to indicate
               completed test run */
            rc = SQLExecute(completed_stmt);
            if (error_s(completed_stmt, rc, NULL)) {
                /* Something went wrong */
                logRecord(FATAL, "write_results",
                          "SQLExecute (on TEST_RUNS) failed -> "
                          "partial data in TIRDB");
                state.phase = END_COMM;
                return -1;
            }
                
            /* Clean up */
                
            rc = SQLFreeHandle(SQL_HANDLE_STMT, resp_time_stmt);
            if (error_s(resp_time_stmt, rc, NULL)) {
                logRecord(ERROR_, "SQLFreeHandle()", "SQLFreeHandle "
                          "failed");
            }
            rc = SQLFreeHandle(SQL_HANDLE_STMT, mqth_stmt);
            if (error_s(mqth_stmt, rc, NULL)) {
                logRecord(ERROR_, "SQLFreeHandle()", "SQLFreeHandle "
                          "failed");
            }
            rc = SQLFreeHandle(SQL_HANDLE_STMT, completed_stmt);
            if (error_s(completed_stmt, rc, NULL)) {
                logRecord(ERROR_, "SQLFreeHandle()", "SQLFreeHandle "
                          "failed");
            }
                
#ifndef LINEAR_RESPONSE_SCALE
            rc = SQLFreeHandle(SQL_HANDLE_STMT, resp_time_percentile_stmt);
            if (error_s(resp_time_percentile_stmt, rc, NULL)) {
                logRecord(ERROR_, "SQLFreeHandle()", "SQLFreeHandle "
                          "failed");
            }
#endif
            DisconnectDB(&tirdb_env, &tirdb, "TIRDB");
        } else if (storeResults == MODE_TO_SQLFILE) {
            fclose(fResults);
        }            
            
        /* state change */
        state.phase = END_COMM;
        return 0;
}
        
/*##**********************************************************************\
 *
 *      send_end_message
 *
 * Send the messages to Control indicating the end
 * of processing. Two messages are sent, first stating
 * the completed processing with the mqth_avg figure and
 * the second message being the log_out message with the
 * the total number of errors during the benchmark.
 *
 * Parameters:
 *      none
 * Return value:
 *      0 - always
 */
int send_end_message( )
{
        int retval;
        int avg_mqth;
        
        /* message_dataS defined in communication.h */
        struct message_dataS msg_completed_data;
        struct message_dataS msg_logout_data;
        
        if (last_used_time_slot > 1) {
            /* Count the average mqth by dividing the summed
               mqth value with the number of time slots along
               the benchmrk duration (exclude rampup time)*/
            avg_mqth = summed_overall_mqth /
                (last_used_time_slot -
                 ((rampup_time*60) / throughput_resolution));
        }
        else {
            avg_mqth = summed_overall_mqth;
        }
        
        msg_completed_data.utime = time(NULL);
        msg_completed_data.sdata.reg.testID = testRunId;
        /* The average mqth of the benchmark just executed */
        msg_completed_data.sdata.reg.data = avg_mqth;
        
        msg_logout_data.utime = time(NULL);
        msg_logout_data.sdata.reg.testID = testRunId;
        /* Total number of errors from the clients and Statistics
           reported to COntrol */
        msg_logout_data.sdata.reg.data =
            clientErrorCount + statisticErrorCount;
        
        /* Send MSG_COMPLETED to Control */
        retval = sendDataS(state.control_socket, STATISTICS_ID,
                           MSG_COMPLETED, &msg_completed_data);
        if (retval != 0) {
            logRecord(WARNING, "send_end_message",
                      "Could not send MSG_COMPLETED to Control");
        }
        else {
            logRecord(DEBUG, "send_end_message",
                      "MSG_COMPLETED sent to Control");
        }
        
        /* Send MSG_LOGOUT to Control */
        retval = sendDataS(state.control_socket, STATISTICS_ID,
                           MSG_LOGOUT, &msg_logout_data);
        if (retval != 0) {
            logRecord(WARNING, "send_end_message",
                      "Could not send MSG_LOGOUT to Control");
        }
        else {
            logRecord(DEBUG, "send_end_message",
                      "MSG_LOGOUT sent to Control");
        }
        
        /* state change */
        state.phase = FINAL;
        return 0;
}

/*##**********************************************************************\
 *
 *      finish_state
 *
 * Does the finalizing procedures
 *
 * Parameters:
 *      none
 * Return value:
 *      none
 */
void finish_state( )
{
        int retval;
        
        /* Finalize the connection to Control */
        retval = disconnectConnection(&g_comm, state.control_socket);
        if (retval) {
            logRecord(WARNING, "finish_state",
                      "Could not disconnect the Control module.");
        }
        /* Some clean up work in communication */
        retval = finalizeCommunication(&g_comm);
        if (retval) {
            logRecord(WARNING, "finish_state",
                      "Could not finalize the communication system.");
        }
        
#ifdef _DEBUG
        /* For TATP software performance analysis
           -> not needed in actual benchmark runs */
        saveMyTimings("STATISTICS_TIMINGS.CSV");
#endif /* _DEBUG */
        
        free(transactions.mqth);
        
        logRecord(DEBUG, "finish_state", "MODULE execution finished.");
        return;
}

/*##**********************************************************************\
 *
 *      logRecord
 *
 * Stores a log record to the log file and shows it on
 * the console. NOTE: parameter position is not used
 * yet (perhaps later on).
 *
 * Parameters:
 *      severity 
 *			either DEBUG,INFO,WARNING,ERROR or FATAL
 *
 *      position 
 *          not used
 *
 *      message
 *          the log message
 *
 * Return value:
 *      none
 */
void logRecord(enum error_severity severity, char *position, char *message)
{
        char logstatus;
        
        switch (severity) {
            case DEBUG:
                logstatus = 'D';
                break;
            case INFO:
                logstatus = 'I';
                break;
            case WARNING:
                logstatus = 'W';
                break;
            case ERROR_:
                logstatus = 'E';
                statisticErrorCount++;
                /* An error causes state change to the result
                   output mode */
                state.phase = OUTPUT;
                break;
            case FATAL:
                logstatus = 'F';
                statisticErrorCount++;
                /* A fatal error causes state change to the
                   'end of communication' mode. We are skipping the
                   result output mode in this case (results are lost) */
                state.phase = END_COMM;
                break;
            default:
                /* Unknown 'severity'ä is handled as a FATAL error */
                logstatus = 'F';
                statisticErrorCount++;
                state.phase = END_COMM;
                break;
        }
        writeLog(logstatus, message);
        return;
}

/*##**********************************************************************\
 *
 *      main
 *
 * The main program which implements the core state machine. The intial
 * state of the machine is PARAMETERS (set by init_state()). The program
 * exits from the FINAL state.
 *
 * Parameters:
 *      argc
 *			the number of command parameters
 *
 *      argv
 *			the command parameters
 * 
 *
 * Return value:
 *      0 - success
 */
int main(int argc, char *argv[])
{        
        if (init_state() == 0) {
#ifdef WIN32
            /* Increase the priority of this process. This was proved in
               practise to boost the performance of the system */
            
/*	SetPriorityClass( GetCurrentProcess(), HIGH_PRIORITY_CLASS); */
#endif
            /* Iterate in the main loop until state.end */
            while (!state.end)
                switch (state.phase) {
                    case MESSAGES:
                        handle_message();
                        break;
                    case PARAMETERS:
                        handle_parameters(argc, argv);
                        break;
                    case INIT_TRANS:
                        init_trans();
                        break;
                    case INIT_COMM:
                        init_communications();
                        break;
                    case OUTPUT:
                        if (argc > STATISTICS_STATIC_ARGC) {
                            /* transactions defined */
                            count_and_store_results();
                        } else {
                            state.phase = END_COMM;
                        }
                        break;
                    case END_COMM:
                        send_end_message();
                        break;
                    case FINAL:
                        finish_state();
                        /* exit the Statistics module */
                        finalizeLog();
                        return 0;
                        break;
                    default:
                        logRecord(ERROR_, "main", "Exit at unknown state!");
                        finalizeLog();
                        return 0;
                        break;
                }
        }
        finalizeLog();
        return 0;
}
