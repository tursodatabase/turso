/**********************************************************************\
**  source       * statistics.h
**  description  * Registers TATP clients willing to send
**                 transaction response times to statistics. Collects
**	               the response times from the active clients and after
**	               the last client have logged off, calculates the
**                 average response time and saves the result either
**	               to TIRDB or the result file.
**	               The client activity is monitored while logged in.
**	               If a client is inactive for too long a log record
**	               is generated.
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifndef STATISTICS_H
#define STATISTICS_H

#include <stdio.h>
#include <time.h>
#include <sys/timeb.h>
#include "communication.h"
#include "util.h"
#include "const.h"

/* How many transaction types we can handle (note that TATP
   defines seven transaction types) */
#define MAX_TRANSACTION_TYPES  20
/* Maximum test length Statistics can hendle is given
   in seconds (86400 seconds = 24 hours */
#define MAX_TEST_LENGTH 86400
/* Maximum number of times we iterate in the message handling
   loop before exiting it */
#define MESSAGE_LOOP_ITERATIONS 1000
/* Few protocol realted constants */
#define MESSAGE_SIZE 256
#define CONNECT_STRING_LENGTH 128
#define DB_ERROR_MSG_SIZE 1024
#define DB_ERROR_CODE_SIZE 32
#define MAX_NUM_OF_DB_ERRORS 1000
/* In certain situation statistics is put to sleep
   (given in milliseconds) */
#define STATISTICS_IDLE 1

/* Define few SQL commands used to update the TIRDB */
#define RESP_TIME_INSERT "INSERT INTO result_response VALUES (?, ?, ?, ?)"
#define RESP_TIME_SCALE_INSERT "INSERT INTO result_response_scale \
(test_run_id, transaction_type, slot, bound, num_of_hits) \
VALUES (?, ?, ?, ?, ?)"
#define RESP_TIME_PERCENTILE_INSERT "INSERT INTO result_response_90percentile \
(test_run_id, transaction_type, resp_time) \
VALUES (?, ?, ?)"
#define MQTH_INSERT "INSERT INTO result_throughput \
(test_run_id, time_slot_num, mqth) VALUES (?, ?, ?)"
/* Note: in current version of TATP we are not reporting
	target db errors (the code is for future needs) */
#define ERROR_INSERT "INSERT INTO target_db_errors \
(test_run_id, \"time\", transaction_type, error_message) \
VALUES (?, ?, ?, ?)"
#define COMPLETED_UPDATE "UPDATE test_runs \
SET test_completed = 1 WHERE test_run_id = ?"

/* The state of a client in the client data structure */
enum client_state {
	NOT_LOGGED_IN,
	LOGGED_IN
};

/* Statistics process is actually a state machine. It can
	be in one of the listed states below. */
enum prog_state {
	PARAMETERS,
	INIT_TRANS,
	INIT_COMM,
	MESSAGES,
	OUTPUT,
	END_COMM,
	FINAL
};

/* The severity of an error encountered. The severity
	defines both the program behaviour and the logging */
enum error_severity {
	DEBUG=55,
	INFO,
	WARNING,
	ERROR_,
	FATAL
};

typedef struct CLIENT_STRUCT {
        int id;
        enum client_state state;
} CLIENT_REC;

/* Note: in current version of TATP we are not reporting
	target db errors (the code is for future needs) */
typedef struct DB_ERROR_STRUCT {
        char transactionType[TRANSACTIONTYPE_SIZE];
#ifdef WIN32
        struct _timeb time;
#else
        struct timeval time;
#endif
        char errorCode[DB_ERROR_CODE_SIZE];
        char errorMessage[DB_ERROR_MSG_SIZE];
} DB_ERROR_REC;

/* The state structure of Statistics. Gives the current state
	of Statistics, lists all the clients logged in and holds
	the socket to communicate with Control */
typedef struct STATE_STRUCT {
        enum prog_state phase;
        int end;
        CLIENT_REC client[MAX_CLIENTS];
        SOCKET control_socket;
} STATE_REC;

/* RESPONSE_LISTS holds the (averaged) response time information
	collected from the clients. It is a 2 dimensional list with
	transaction types in one dimension and the resposne times
	(up to MAX_RESP_TIME_SLOTS) in the other dimension */
typedef unsigned int
	RESPONSE_LISTS[MAX_TRANSACTION_TYPES][MAX_RESP_TIME_SLOTS];
/* Note: in current version of TATP we are not reporting
	target db errors (the code is for future needs) */
typedef DB_ERROR_REC DB_ERRORS[MAX_NUM_OF_DB_ERRORS];

/* The main structure to hold all the transaction statistics
	(both MQTh and response times per tranasction type) */
typedef struct TRANS_STRUCT {
        int *mqth;
        RESPONSE_LISTS resp_bounds;
        RESPONSE_LISTS resp;
        /* Note: in current version of TATP we are not reporting
           target db errors (the code is for future needs) */
        DB_ERRORS dbErrors;
} TRANS_REC;

typedef char TransactionName[TRANSACTIONTYPE_SIZE];

/* Globals within Statistics */
int testRunId;
STATE_REC state;
TRANS_REC transactions;
char tirdbConnectString[CONNECT_STRING_LENGTH];
FILE *fResults = NULL;
char resultFileName[W_L];

int clients_online = 0;  /* A counter of the clients on-line */
int clientErrorCount;    /* Sums all the fatal errors sent by clients
                            in their LOGOUT messages */
int dbErrorCount;        /* The number of target database errors */
int statisticErrorCount; /* The number of statistics errors */
int rampup_time;         /* The rampup time given by Control (in seconds) */
int rampUpTimeOn;        /* Indicator for the rampup time */

int storeResults = MODE_TO_LOGS_ONLY;

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

/* The throughput resolution from TDF file (in seconds) */
int throughput_resolution;
/* number of time slots -> (MAX_TEST_LENGTH / throughput_resolution) */
int num_of_time_slots;
/* The last time slot we have received data for */
int last_used_time_slot;
/* Summed mqth value over the benchmark time */
unsigned long summed_overall_mqth;
/* Transaction type name mapping */
TransactionName transaction_names[MAX_TRANSACTION_TYPES];

void logRecord(enum error_severity severity, char *position, char *message);
int init_state();
int handle_parameters(int argc, char *argv[]);
int init_trans();
int init_communications();
int handle_message();
int count_and_store_results();
int send_end_message();
void finish_state();
int handle_registration(int sender_id, struct message_dataS *data);
int handle_logout(int sender_id, struct message_dataS *data);
int handle_mqthMsg(int sender_id, struct message_dataS *data);
int handle_resptimeMsg(int sender_id, struct message_dataS *data);

#endif /* STATISTICS_H */
