/**********************************************************************\
**  source       * remcontrol.c
**  description  * The remcontrol module contains the
**		           remote control specific functions.
**		           'remote control' is in responsible of
**		           - communicating with the main control
**			       - starting the clients in the 'remote' machine
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
**********************************************************************/

#include "remcontrol.h"
#include "timer.h"
#include "tatpversion.h"
#include <time.h>

extern communication_t g_comm;
extern int controlModuleMode;
extern int controlModulePortNumber;

/* The remote machines structure. We store the information starting
from index 1 (and not 0) */
struct remoteControl remControls[MAX_NUM_OF_REMOTE_COMPUTERS];

/* Socket connections for the clients (MAX_CONNECTIONS
	equals max num of clients per remote */
SOCKET clientScks[MAX_CONNECTIONS+1];

/*##**********************************************\
 *
 *      remoteControl
 *
 * The 'remote control' main loop. Waits requests from the
 * 'main control'. The possible requests are
 *   1) PING requests
 *	 2) test parameters
 *   3) start the TATP clients
 *   4) TIME request
 *   5) start the test
 *   6) send the log files to the main control
 *
 * Parameters:
 *      none
 *
 * Return value:
 *      none
 */
void remoteControl() {
        int retval = 0, i;
        /* My own control identifier. A number that is less than -2.
           Given in the PING message from the main control */
        int myControlID;
        /* Socket for the main control of the benchmark */
        SOCKET mainControlSocket;
        /* Sender ID of the received message */
        int senderID;
        /* Type of the received message. */
        int messageType;
        /* Struct to hold the received data */
        struct message_dataS data;
        /* message buffer */
        char msg[W_L];
        /* buffer for filenames */
        char filename[W_L];
        /* The test parameters */
        struct clientStartParameters csp;
        /* Timer to count of the time used to send and receive TIME
           messages from the clients */
        struct timertype_t clientSendTimer;
        __int64 checkedTime;
        /* The longest time in ms a client used for the call-back loop */
        int longestClientResponse;
#ifdef ACCELERATOR
        char c;
#endif
        /* wait time for accelerator to start */
        int waitDatabaseStart = DEFAULT_ACCELERATOR_WAIT_TIME;
        
        initTimer(&clientSendTimer, TIMER_MILLISECOND_SCALE);
        
        /* Open the log file in append mode */
        if (createLog(DEFAULT_LOG_FILE_NAME) == -1) {
            writeLog('I', "Could not initialize the log file. Exiting...");
            return;
        };
        
        sprintf(msg, "*** Start TATP Remote v. %s ***", TATPVERSION);
        writeLog('I', msg);
        writeLog('I', "Waiting for 'Main Control'...");
        
        for (i = 0; i <= MAX_CONNECTIONS; i++) {
            clientScks[i] = 0;
        }
        csp.numOfClients = -1;
        myControlID = 0;
        mainControlSocket = 0;
        while (1) {
            retval = receiveDataS(&g_comm, &senderID, &messageType, &data);
            if (retval == 0) {
                if (senderID != MAIN_CONTROL_ID) {
                    sprintf(msg,
                            "Received a message from an unexpected sender "
                            "'%d'",
                            senderID);
                    writeLog('F', msg);
                    return;
                }
                else {
                    switch(messageType) {
                        case MSG_FILE:
                            /* A file coming */
                            if (receiveFile(data.sdata.file.fileFragment,
                                            NULL)) {
                                writeLog('F', "Error receiving a file from "
                                         "Main Control.");
                                return;
                            }
                            writeLog('D', "Received file");
                            break;
                        case MSG_PING:
                            writeLog('D', "Received PING request from Main "
                                     "Control.");
                            /* Got my ID in the PING message */
                            myControlID = data.sdata.reg.data;
                            mainControlSocket = createConnection(
                                                     data.sdata.reg.ip,
                                                     MAIN_CONTROL_PORT);
                            if (!mainControlSocket) {
                                writeLog('F', "Failed to create a socket "
                                         "connection to Main Control.");
                                return;
                            }
                            /* Send response to the PING request */
                            sendDataS(mainControlSocket, myControlID,
                                      MSG_PING, &data);
                            /* Initialize the client parameters before
                               starting to receive messages carrying values
                               for them */
                            csp.db_connect[0] = '\0';
                            csp.transaction_file[0] = '\0';
                            csp.statistics_host[0] = '\0';
                            csp.db_schemaname[0] = '\0';
                            csp.connection_init_file[0] = '\0';
                            csp.rampup = UNDEFINED_VALUE;
                            csp.rampupPlusLimit = UNDEFINED_VALUE;
                            csp.verbose = UNDEFINED_VALUE;
                            csp.population_size = UNDEFINED_VALUE;
                            csp.uniform = UNDEFINED_VALUE;
                            csp.testRunId = UNDEFINED_VALUE;
                            csp.namesAndProbs[0] = '\0';

                            csp.check_targetdb = UNDEFINED_VALUE;
                            csp.serial_keys = UNDEFINED_VALUE;
                            csp.commitblock_size = UNDEFINED_VALUE;

                            csp.db_schemafilename[0] = '\0';
                            csp.reportTPS = UNDEFINED_VALUE;
                            csp.detailedStatistics = UNDEFINED_VALUE;
                            csp.waitDatabaseStart = -1*INT_MAX;
                            
                            csp.firstClient = UNDEFINED_VALUE;
                            csp.numOfClients = UNDEFINED_VALUE;
                            csp.numOfProcesses = UNDEFINED_VALUE;
                            csp.min_subs_id = UNDEFINED_VALUE;
                            csp.max_subs_id = UNDEFINED_VALUE;
                            csp.workDir[0] = '\0';
                            csp.tr_amount = UNDEFINED_VALUE;

                            csp.throughput_resolution = UNDEFINED_VALUE;
                            csp.operation_mode = RUN; /* TODO: other modes */

                            break;
                        case MSG_TESTPARAM:
                            if (data.sdata.testparam.data[0]) {
                                sprintf(msg, "Received TESTPARAM from Main "
                                        "Control (value '%s')",
                                        data.sdata.testparam.data);
                                writeLog('D', msg);
                                /* Put the parameter to the right place */
                                resolveTestParameters(&data, &csp);
                            }
                            waitDatabaseStart = csp.waitDatabaseStart;
                            break;
                        case MSG_SPAWNCLIENTS:
                            writeLog('D', "Received SPAWNCLIENTS request "
                                     "from Main Control.");
                            if (validTestParameters(&csp)) {
                                /* Valid parameters -> start the clients */
                                if (spawnClients(&csp) != 0) {
                                    message('E', "Could not start all the "
                                            "client processes");
                                    message('E', "Not all the client "
                                            "processes running");
                                }
                                else {
#ifdef ACCELERATOR
                                    if (waitDatabaseStart > 0) {
                                        /* wait for accelerator to load
                                           the database */
                                        sprintf(msg, "Waiting %d seconds for "
                                                "the database to start up "
                                                "before getting answers from "
                                                "clients",
                                                waitDatabaseStart);
                                        writeLog('I', msg);
                                        msSleep(waitDatabaseStart*1000);
                                    } else if (waitDatabaseStart == 0) {
                                        writeLog('I', "Press enter when "
                                                 "the database has started.");
                                        c = getchar();
                                    }
#endif
                                    if (getRemClientResponses(
                                                &csp,
                                                waitDatabaseStart)) {
                                        /* Send INTR to the main control */
                                        sendDataS(mainControlSocket,
                                                  myControlID,
                                                  MSG_INTR, &data);
                                        sprintf(msg, "Problems starting "
                                                "the clients for the test "
                                                "run %d",
                                                csp.testRunId);
                                        writeLog('E', msg);
                                    }
                                    else {
                                        /* Send OK to the main control */
                                        sendDataS(mainControlSocket,
                                                  myControlID,
                                                  MSG_OK, &data);
                                        sprintf(msg, "%d clients started "
                                                "for the test run %d",
                                                csp.numOfClients,
                                                csp.testRunId);
                                        writeLog('I', msg);
                                    }
                                    if (createClientConnections(
                                                csp.numOfClients,
                                                csp.firstClient)) {
                                        message('E', "Could not create "
                                                "connection to all client "
                                                "threads");
                                    }
                                }
                            }
                            else {
                                message('E',
                                        "Error in the test parameters received "
                                        "from Main Control");
                            }
                            break;
                        case MSG_TIME:
                            startTimer(&clientSendTimer);
                            sprintf(msg, "Received TIME message from "
                                    "Main Control "
                                    "(value '%d')",
                                    data.sdata.reg.data);
                            writeLog('D', msg);
                            /* Send TIME messages to and receive responses
                               from all the clients of this Remote */
                            longestClientResponse = 0;
                            if (timeClientResponses(&csp,
                                                    &longestClientResponse,
                                                    myControlID,
                                                    data.sdata.reg.data,
                                                    &clientSendTimer)) {
                                /* Something wrong with the client
                                   communication */
                                message('E', "Test time propagation to clients "
                                        "failed");
                                message('E', "Client synchronization can not "
                                        "be guaranteed");
                            }
                            readTimer(&clientSendTimer, &checkedTime);
                            /* Eliminate the time we used to process the client
                               calls and responses */
                            /* Safe to cast from __int64 to int 
                               (value is small) */
                            data.sdata.reg.data =
                                data.sdata.reg.data + checkedTime;

                            /* Add the time of the longest call-back loop
                               of a client */
                            data.sdata.reg.data =
                                data.sdata.reg.data-longestClientResponse;

                            /* Send response to the TIME request */
                            sendDataS(mainControlSocket, myControlID,
                                      MSG_TIME, &data);
                            break;
                        case MSG_STARTTEST:
                            writeLog('D', "Received STARTTEST message from "
                                     "Main Control");
                            /* echo STARTTEST to clients */
                            for (i = 0; i < csp.numOfClients; i++) {
                                sendDataS(clientScks[i], myControlID,
                                          MSG_STARTTEST, &data);
                            }
                            /* Disconnect all the client connections */
                            disconnectClientConnections();
                            writeLog('I', "Waiting for 'Main Control'...");
                            break;
                        case MSG_INTR:
                            writeLog('W', "Interruption request received from "
                                     "Main Control");
                            for (i = 0; i < csp.numOfClients; i++) {
                                if (clientScks[i]) {
                                    sendDataS(clientScks[i], myControlID,
                                              MSG_INTR, &data);
                                }
                            }
                            /* Disconnect all the client connections */
                            disconnectClientConnections();
                            writeLog('I', "Waiting for 'Main Control'...");
                            break;
                        case MSG_CLEAN:
                            writeLog('D', "Received CLEAN message from "
                                     "Main Control");
                            /* Discard child process entries */
                            cleanUpClients(csp.numOfProcesses);
                            /* Delete client log files */
                            for (i = 0; i < csp.numOfClients; i++) {
                                sprintf(filename, CLIENT_LOGFILENAME_FORMAT,
                                        i + csp.firstClient);
                                if (remove(filename) != 0){
                                    sprintf(msg, "Error deleting client log "
                                            "file '%s'", filename);
                                    message('E', msg);
                                }
                            }
                            break;
                        case MSG_LOGREQUEST:
                            writeLog('D', "Received LOGREQUEST message from "
                                     "Main Control");
                            /* send all log files to main control and then OK */
                            sendLogFiles(mainControlSocket, myControlID,
                                         csp.firstClient, csp.numOfClients);
                            sendDataS(mainControlSocket, myControlID, MSG_OK,
                                      &data);
                            /* this is the last message that is expected from
                               the main control so close the connection from the
                               remote control side */
                            if (mainControlSocket) {
                                portable_closesocket(&mainControlSocket);
                            }
                            break;
                        default:
                            sprintf(msg,
                                    "Received an unexpected message '%d' from "
                                    "Main Control",
                                    messageType);
                            writeLog('F', msg);
                            return;
                    }
                }
            }
            else {
                sprintf(msg,
                        "Error %d at receiveDataS() while waiting message from "
                        "Main Control",
                        retval);
                writeLog('F', msg);
                return;
            }
        }
}

/*##**********************************************************************\
 *
 *      sendLogFiles
 *
 * Sends the log files to the socket defined as parameter.
 *
 * Parameters:
 *      sck 
 *          Socket to send the log files to
 *
 *      myID 
 *          identifier of the caller control
 *
 *      firstClient 
 *			the number of the first client
 *
 *      numOfClients 
 *          number of clients
 *
 * Return value:
 *      0 - success 
 */
int sendLogFiles(SOCKET sck, int myID, int firstClient, int numOfClients)
{
        char filename[W_L];
        int i;
        char msg[W_L];
        
        /* send all client logs */
        for (i = firstClient; i < firstClient+numOfClients; i++) {
            sprintf(filename, CLIENT_LOGFILENAME_FORMAT, i);
            if (sendFileToSocket(sck, myID, filename, LOGFILE)) {
                sprintf(msg, "Error sending a log file '%s' "
                        "to the main control",
                        filename);
                message('E', msg);
            }
        }
        if (sendFileToSocket(sck, myID, DEFAULT_LOG_FILE_NAME, LOGFILE)) {
            sprintf(msg, "Error sending a log file '%s' to the main control",
                    DEFAULT_LOG_FILE_NAME);
            message('E', msg);
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      InitRemotesStruct
 *
 * Initializes the structure that holds information about the remote
 * machines, that is, the other machines that run TATP clients.
 *
 * Parameters :
 *      none
 * Return value:
 *      none
 */
void InitRemotesStruct() {
        int i;
        for (i = 0; i < MAX_NUM_OF_REMOTE_COMPUTERS; i++) {
            remControls[i].name[0] = '\0';
            remControls[i].ip[0] = '\0';
            remControls[i].targetDBdsn[0] = '\0';
            remControls[i].sck = 0;
            remControls[i].remoteControlId = 0;
        }
}

/*##**********************************************************************\
 *
 *      resolveTestParameters
 *
 * Resolves the test parameters received via the communication module.
 *
 * Parameters:
 *      data 
 *			The data received via network
 *
 *      csp 
 *			client data structure
 *
 * Return value:
 *      none
 */
void resolveTestParameters(struct message_dataS *data,
                           struct clientStartParameters *csp)
{
        /* Resolve the parameters. Note that the order of the if
           branches is crucial. */
        if (csp->numOfClients == UNDEFINED_VALUE) {
            csp->numOfClients = atoi(data->sdata.testparam.data);
        }
        else if (csp->min_subs_id == UNDEFINED_VALUE) {
            csp->min_subs_id = atoi(data->sdata.testparam.data);
        }
        else if (csp->max_subs_id ==  UNDEFINED_VALUE) {
            csp->max_subs_id = atoi(data->sdata.testparam.data);
        }
        else if (csp->firstClient ==  UNDEFINED_VALUE) {
            csp->firstClient = atoi(data->sdata.testparam.data);
        }
        else if (csp->numOfProcesses == UNDEFINED_VALUE) {
            csp->numOfProcesses = atoi(data->sdata.testparam.data);
        }
        else if (!csp->db_connect[0]) {
            strncpy(csp->db_connect, data->sdata.testparam.data, W_L);
        }
        else if (!csp->db_schemaname[0]) {
            strncpy(csp->db_schemaname, data->sdata.testparam.data, W);
        }
        else if (!csp->db_schemafilename[0]) {
            strncpy(csp->db_schemafilename, data->sdata.testparam.data, W);
        }
        else if (!csp->workDir[0]) {
            strncpy(csp->workDir, data->sdata.testparam.data, W_L);
        }
        else if (!csp->connection_init_file[0]) {
            strncpy(csp->connection_init_file, data->sdata.testparam.data, W_L);
        }
        else if (csp->population_size == UNDEFINED_VALUE) {
            csp->population_size = atoi(data->sdata.testparam.data);
        }
        else if (csp->uniform == UNDEFINED_VALUE) {
            csp->uniform = atoi(data->sdata.testparam.data);
        }
        else if (csp->rampup == UNDEFINED_VALUE) {
            csp->rampup = atoi(data->sdata.testparam.data);
        }        
        else if (csp->check_targetdb == UNDEFINED_VALUE) {
            csp->check_targetdb = atoi(data->sdata.testparam.data);
        }
        else if (csp->serial_keys == UNDEFINED_VALUE) {
            csp->serial_keys = atoi(data->sdata.testparam.data);
        }
        else if (csp->commitblock_size == UNDEFINED_VALUE) {
            csp->commitblock_size = atoi(data->sdata.testparam.data);
        }
        else if (csp->rampupPlusLimit == UNDEFINED_VALUE) {
            csp->rampupPlusLimit = atoi(data->sdata.testparam.data);
        }
        else if (!csp->statistics_host[0]) {
            strncpy(csp->statistics_host, data->sdata.testparam.data, W);
        }
        else if (csp->testRunId == UNDEFINED_VALUE) {
            csp->testRunId = atoi(data->sdata.testparam.data);
        }
        else if (csp->tr_amount == UNDEFINED_VALUE) {
            csp->tr_amount = atoi(data->sdata.testparam.data);
        }
        else if (!csp->transaction_file[0]) {
            strncpy(csp->transaction_file, data->sdata.testparam.data, W_L);
        }
        else if (csp->verbose == UNDEFINED_VALUE) {
            csp->verbose = atoi(data->sdata.testparam.data);
        }
        else if (csp->throughput_resolution == UNDEFINED_VALUE) {
            csp->throughput_resolution = atoi(data->sdata.testparam.data);
        }
        else if (csp->reportTPS == UNDEFINED_VALUE) {
            csp->reportTPS = atoi(data->sdata.testparam.data);
        }
        else if (csp->detailedStatistics == UNDEFINED_VALUE) {
            csp->detailedStatistics = atoi(data->sdata.testparam.data);
        }
        else if (csp->waitDatabaseStart == (-1*INT_MAX)) {
            csp->waitDatabaseStart = atoi(data->sdata.testparam.data);
        }
        else {
            /* Concatenate to the transaction and probablity structure */
            strncat(csp->namesAndProbs, data->sdata.testparam.data,
                    MAX_MESSAGE_SIZE);
            strncat(csp->namesAndProbs, " ", 1);
        }
}

/*##**********************************************************************\
 *
 *      validTestParameters
 *
 * Check the validity of the test parameters received via
 * the communication module.
 *
 * Parameters:
 *      csp 
 *			client data structure
 *
 * Return value:
 *      1 - valid parameters
 *      0 - not valid parameters
 */
int validTestParameters(struct clientStartParameters *csp)
{
        if (csp->rampup < 0) {
            writeLog('E', "Test parameter error: rampup time can not be "
                     "negative");
            return 0;
        }
        if (csp->rampupPlusLimit <= 0) {
            writeLog('E', "Test parameter error: test time can not be zero");
            return 0;
        }
        if (strlen(csp->db_connect) == 0) {
            writeLog('E', "Test parameter error: target database ODBC "
                     "connect string not defined");
            return 0;
        }
        if (csp->firstClient < 1) {
            writeLog('E', "Test parameter error: the number of the first "
                     "client can not be zero");
            return 0;
        }
        if (strlen(csp->namesAndProbs) == 0) {
            writeLog('E', "Test parameter error: no transaction defined");
            return 0;
        }
        if (csp->numOfClients < 1) {
            writeLog('E', "Test parameter error: number of clients have to be "
                     "at least 1");
            return 0;
        }
        if (csp->numOfProcesses < 1) {
            writeLog('E', "Test parameter error: number of processes "
                     "have to be 1 or more");
            return 0;
        }
        if (csp->population_size < 1) {
            writeLog('E', "Test parameter error: population size have to be "
                     "at least 1");
            return 0;
        }
        if (csp->max_subs_id < csp->min_subs_id) {
            writeLog('E', "Test parameter error: max subscriber id smaller "
                     "than min subscriber id");
            return 0;
        }
        if (csp->uniform != 0
            && csp->uniform != 1) {
            writeLog('E', "Test parameter error: uniform have to be one "
                     "of [0, 1]");
            return 0;
        }
        if (strlen(csp->statistics_host) == 0) {
            writeLog('E', "Test parameter error: no host of the statistics "
                     "process given");
            return 0;
        }
        if (csp->testRunId < 0) {
            writeLog('E', "Test parameter error: no test run identifier given");
            return 0;
        }
        if (csp->throughput_resolution < 1) {
            writeLog('E', "Test parameter error: throughput resolution has "
                     "to be at least 1");
            return 0;
        }
        if (strlen(csp->transaction_file) == 0) {
            writeLog('E', "Test parameter error: no transaction file "
                     "name given");
            return 0;
        }
        if (csp->verbose < 0) {
            writeLog('E', "Test parameter error: no verbosity level given");
            return 0;
        }
        return 1;
}

/*##**********************************************************************\
 *
 *      getRemClientResponses
 *
 * Wait for OK messages from the clients (indicates that they are
 * succesfully started)
 *
 * Parameters:
 *      csp
 *			client start parameters struct
 *
 *      waitDatabaseStart 
 *          time to wait for client messages 
 *
 * Return value:
 *      1 - error
 *      0 - success
 */
int getRemClientResponses(struct clientStartParameters *csp,
                          int waitDatabaseStart)
{
        char msg[W_L];
        char buf[MAX_MESSAGE_SIZE];
        int missingResponses, j, retval, senderID, messageType, loopCounter;
        struct message_dataS data;
        short clientUp[MAX_CONNECTIONS+1];
        
        for (j = csp->firstClient;
             j < csp->firstClient + csp->numOfClients; j++) {
            clientUp[j] = 0;
        }
        
        /* Receive the OK message from all the clients */
        missingResponses = 1;
        message('D', "Waiting for OK messages from the local clients");
        loopCounter =
            MAX_CONTROL_RESPONSE_WAIT_TIME / MESSAGE_RESPONSE_LOOP_SLEEP_TIME;
        while (missingResponses && loopCounter > 0) {
            retval = receiveMessage(&g_comm, buf);
            if (retval > 0) {
                if (decodeMessage(buf, &senderID, &messageType, &data) != 0) {
                    message('E', "Internal error from the communication "
                            "module");
                    return -1;
                }
                if (messageType != MSG_OK) {
                    sprintf(msg, "Unexpected message received from %d",
                            senderID);
                    message('W', msg);
                    return -1;
                }
                else {
                    /* Lets not decrease the loop counter if we actually
                       got an OK */
                    loopCounter++;
                }
                clientUp[senderID] = 1;
            }
            else if (retval < 0) {
                message('E', "Internal error from the communication module");
                return -1;
            }
            /* Check if we are still waiting for OK messages */
            missingResponses = 0;
            for (j = csp->firstClient; j < csp->firstClient + csp->numOfClients;
                 j++) {
                if (clientUp[j] == 0) {
                    missingResponses = 1;
                    break;
                }
            }
            if (missingResponses == 0) {
                /* All clients have sent an OK message  */
                message('D', "Got OK message from all local clients");
                return 0;
            }
            SLEEP(MESSAGE_RESPONSE_LOOP_SLEEP_TIME); /* time for a short nap */
            /* loop continuously if negative wait time given */
            if (waitDatabaseStart >= 0) {
                loopCounter--;
            }
        }
        /* We did not get all the responses within reasonable time */
        message('E', "Not all the clients started");
        return -1;
}

/*##**********************************************************************\
 *
 *      timeClientResponses
 *
 * Sends TIME messages to all the remote's clients and waits for the
 * responses. The time is measured for this communication loop and the
 * slowest communication loop time is returned to the caller in the
 * parameter.
 *
 * Parameters:
 *      csp
 *          pointer to client data structure
 *
 *      longestClientResponse
 *          pointer to integer to store
 *          slowest response time from clients
 *
 *      myControlID
 *	        identifier of the caller in socket communications
 *
 *      mainControlTime 
 *          time value from Main Control
 *
 *      remoteInternalTime 
 *          pointer to timer struct
 *
 * Return value:
 *      1 - error
 *      0 - success
 */
int timeClientResponses(struct clientStartParameters *csp,
                        int *longestClientResponse, int myControlID,
                        int mainControlTime,
                        struct timertype_t *remoteInternalTime)
{
        int i, retval, senderID, messageType, synchMistake;
        __int64 timeCheck, loopTime;
        struct message_dataS data;
        struct timertype_t loopTimer;
        char buf[MAX_MESSAGE_SIZE];
        char txt_buf[256];
        
        initTimer(&loopTimer, TIMER_MILLISECOND_SCALE);
        
        *longestClientResponse = 0;
        for (i = 0; i < csp->numOfClients; i++) {
            readTimer(remoteInternalTime, &timeCheck);
            /* Safe cast (the value of __int64 is small enough) */
            data.sdata.reg.data = mainControlTime + timeCheck;
            data.utime = time(NULL);
            data.sdata.reg.ip[0] = '\0';  /* not used with MSG_TIME */
            data.sdata.reg.testID = 0;    /* not used with MSG_TIME */
            sendDataS(clientScks[i], myControlID, MSG_TIME, &data);
            
            /* Receive the response from a client */
            loopTime = 0;
            startTimer(&loopTimer);
            while (loopTime < MAX_CLIENT_RESPONSE_WAIT_TIME) {
                retval = receiveMessage(&g_comm, buf);
                if (retval > 0) {
                    /* We got a message -> read the test time first */
                    readTimer(remoteInternalTime, &timeCheck);
                    
                    if (decodeMessage(buf, &senderID,
                                      &messageType, &data) != 0) {
                        message('E', "Internal error from the communication "
                                "module");
                        return -1;
                    }
                    if (messageType != MSG_TIME) {
                        sprintf(txt_buf, "Unexpected message received from "
                                "client %d",
                                senderID);
                        message('W', txt_buf);
                        /* Try to continue */
                        continue;
                    }
                    /* we got TIME message from the right sender. */
                    synchMistake =
                        (mainControlTime + timeCheck) - data.sdata.reg.data;
                    if (synchMistake > *longestClientResponse) {
                        *longestClientResponse = synchMistake;
                    }
                    
                    sprintf(txt_buf, "Client synch. error less than %d ms",
                            (synchMistake+1) / 2);
                    message('D', txt_buf);
                    break;
                }
                else if (retval < 0) {
                    message('E', "Internal error from the "
                            "communication module");
                    return -1;
                }
                readTimer(&loopTimer, &loopTime);
            }
            if (loopTime >= MAX_CLIENT_RESPONSE_WAIT_TIME) {
                /* We did not get an answer from a remote in time */
                sprintf(txt_buf, "Client did not response to the TIME "
                        "message in %d ms",
                        MAX_CLIENT_RESPONSE_WAIT_TIME);
                message('E', txt_buf);
                return -1;
            }
            
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      createClientConnections
 *
 * Create socket connections to all the clients.
 * Clients listen to the ports starting from CLIENT_PORT_BASE such
 * a way that the client with lowest id listens the port
 * CLIENT_PORT_BASE, the next CLIENT_PORT_BASE+1 and so on.
 *
 * Parameters:
 *      numOfClients 
 *          number of clients
 *      firstClientNum
 *          client ID number of the first client
 *
 * Return value:
 *      0 - success
 *     -1 - error
 */
int createClientConnections(int numOfClients, int firstClientNum)
{
        int i;
        int offset;
        
        message('D', "Creating connections to clients");
        for (i = 0; i < numOfClients; i++) {
            if (clientScks[i] == 0) {

                if (controlModuleMode == MODE_REMOTE_CONTROL) {
                    offset = 0;
                } else {                    
                    offset = firstClientNum - 1;
                }
                clientScks[i] = createConnection(
                        "localhost",
                        (unsigned short)(CLIENT_PORT_BASE
                                         + (offset + i)));
                if (clientScks[i] == 0) {
                    /* Connection failed */
                    message('E', "Could not connect to all clients");
                    return -1;
                }
            }
            else {
                message('E', "Internal error: socket handle not "
                        "free as expected");
                return -1;
            }
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      disconnectClientConnections
 *
 * Closes all communication sockets opened towards clients.
 *
 * Parameters:
 *      none
 * Return value:
 *      none
 */
void disconnectClientConnections( )
{
        int i;
        for (i = 0; clientScks[i] != 0; i++) {
            portable_closesocket(&(clientScks[i]));
        }
}
