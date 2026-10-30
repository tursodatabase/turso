/**********************************************************************\
**  source       * const.h
**  description  * Global constants. The constant defined
**		           here are either used by several modules or they
**		           are otherwise considered TATP wide constants.
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public 
**  License 1.0 as published by the Open Source Initiative (OSI). 
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifndef CONST_H
#define CONST_H

#define FILENAME_LENGTH 256

#define DEFAULT_STATISTICS_RESULTFILENAME "tatp_results.sql"

#define DEFAULT_DBUSER_UID "dba"
#define DEFAULT_DBUSER_PWD "dba"

/* Name of the initialization file */
#define DEFAULT_INIFILE_NAME "tatp.ini"
/* Default log file name for the control module */
#define DEFAULT_LOG_FILE_NAME "tatp.log"
/* Statistics module lof file name */
#define STATISTICS_LOG_FILE_NAME "statistics.log"
/* directory where to store test sessions logs */
#define LOG_ARCHIVE_PATH "logs"

#define CLIENT_LOGFILENAME_FORMAT "client%d.log"

#define DEFAULT_DBSCHEMAFILE_NAME "targetDBSchema.sql"
#define DEFAULT_TRANSACTIONFILE_NAME "tr_mix_solid.sql"

#define UNDEFINED_VALUE -1

#define PRINT_COLOR_BLACK 0
#define PRINT_COLOR_BLUE 1
#define PRINT_COLOR_GREEN 2
#define PRINT_COLOR_CYAN 3
#define PRINT_COLOR_RED 4
#define PRINT_COLOR_MAGENTA 5
#define PRINT_COLOR_YELLOW 6
#define PRINT_COLOR_WHITE 7

/* Messaging colors */
#define DEFAULT_COLOR_CLIENT PRINT_COLOR_YELLOW
#define DEFAULT_COLOR_CONTROL PRINT_COLOR_GREEN
#define DEFAULT_COLOR_STATISTICS PRINT_COLOR_CYAN

/* default timeformat for log files */
#define STRF_TIMEFORMAT "%Y-%m-%d %H:%M:%S"

/* Default client synch threshold value (in milliseconds) */
#define DEFAULT_CLIENT_SYNCH_THRESHOLD 10
/* Max time in milliseconds we wait response message from 
a Remote control (used in the test starting phase between main 
control and remote control ) */
#define MAX_CONTROL_RESPONSE_WAIT_TIME 30000 /* 30 seconds */
/* Max time in milliseconds we wait response message from 
a client (used in the test starting phase between control and client) */
#define MAX_CLIENT_RESPONSE_WAIT_TIME 30000 /* 60 seconds */
/* We use this to define the sleep time while waiting for responses
from clients and remotes (milliseconds) */
#define MESSAGE_RESPONSE_LOOP_SLEEP_TIME 10

#ifndef LINEAR_RESPONSE_SCALE
/* number of logarithmic slots per an order of magnitude */
#define LOG_RESP_TIME_SLOTS_PER_DECADE 7
/* minimal response time bound in microseconds, the very first 
   slot is (0, LOG_RESP_TIME_MIN_BOUND) */
#define LOG_RESP_TIME_MIN_BOUND 10 
/* number of decades in allowable response time range */
#define LOG_RESP_TIME_DECADES 5
/* total number of logarithmic slots */
#define MAX_RESP_TIME_SLOTS (LOG_RESP_TIME_DECADES \
                             * LOG_RESP_TIME_SLOTS_PER_DECADE + 1)

#else
/* The maximum resposne time in milliseconds we can handle */
#define MAX_RESP_TIME_SLOTS 10000

#endif /* LINEAR_RESPONSE_SCALE */

#define FILENAME_LENGTH 256

/* Maximum number of remote machines running clients */
#define MAX_NUM_OF_REMOTE_COMPUTERS 100
/* Maximum number of clients that can be used (if you change 
   this note that the communication module has a similar 
   constant for maximum clients <- change that too) */
#define MAX_CLIENTS 1024

/* Character string lengths as defined in TIRDB (column value lengths) */
/* used also for other purposes, for example W_L for filenames */
#define W 128
#define W_L 256
#define W_EL 4096

/* Error codes returned by functions, zero means success. */
#define E_NOT_OK -1
#define E_OK 0
#define E_WARNING 100      /* General warning */
#define E_NO_KEYWORD 101   /* Keyword did not match */
#define E_ERROR 200        /* General error */
#define E_FATAL 300        /* General fatal error */

#define DEFAULT_VERBOSITY_LEVEL 4 /* level 4 is 'I' == informative */
/* levels are defined in util.h */

/* result storing modes */
enum result_modes {
    MODE_TO_LOGS_ONLY,
    MODE_TO_SQLFILE,
    MODE_TO_TIRDB
};

/* The command type in the [Test sequnce] section in TDF */
typedef enum test_sequence_cmd_type {
    POPULATE,
    POPULATE_CONDITIONALLY,
    POPULATE_INCREMENTALLY,
    RUN,
    RUN_DEDICATED,
    EXECUTESQL,
    EXECUTESQLFILE,
    SLEEP,
    NOP,
} cmd_type;

#define SUBNBR_LENGTH 15
#define AI_DATA3_LENGTH 3
#define AI_DATA4_LENGTH 5
#define SF_DATAB_LENGTH 5

/* TIRDB character column lengths for ODBC drivers
   that require its explicit value (namely, Informix) */
#define TIRDB_CONFIG_ID_LEN 128
#define TIRDB_CONFIG_NAME_LEN 128
#define TIRDB_CONFIG_FILE_LEN 32000
#define TIRDB_CONFIG_COMMENTS_LEN 4096
#define TIRDB_SESSION_NAME_LEN 255
#define TIRDB_SESSION_AUTHOR_LEN 32
#define TIRDB_DB_NAME_LEN 32
#define TIRDB_DB_VERSION_LEN 32
#define TIRDB_OS_NAME_LEN 32
#define TIRDB_OS_VERSION_LEN 32
#define TIRDB_SOFTWARE_VERSION_LEN 32
#define TIRDB_SESSION_COMMENTS_LEN 255
#define TIRDB_TEST_NAME_LEN 255
#define TIRDB_TRANSACTION_NAME_LEN 32 
#define TIRDB_HOST_NAME_LEN 255
#define TIRDB_HOST_ADDRESS_LEN 64

#define ARGV_WORKDIR 1
#define ARGV_TRANSACTION_FILE 2
#define ARGV_CONNECTION_INIT_SQL_FILENAME 3
#define ARGV_DBSCHEMAFILENAME 4
#define ARGV_DBSCHEMANAME 5
#define ARGV_OPERATION_TYPE 6
#define ARGV_TEST_DSN 7
#define ARGV_SERIAL_KEYS 8
#define ARGV_COMMITBLOCK_SIZE 9
#define ARGV_CHECK_TARGETDB 10
#define ARGV_RAMPUP_TIME 11
#define ARGV_TEST_TIME 12
#define ARGV_LOG_VERBOSITY 13
#define ARGV_THROUGHPUT_RESOLUTION 14
#define ARGV_STATISTICS_IP 15
#define ARGV_CLIENT_ID 16
#define ARGV_CLIENT_TCP_LISTEN_PORT 17
#define ARGV_CONTROL_TCP_LISTEN_PORT 18
#define ARGV_TEST_ID 19
#define ARGV_POPULATION_SIZE 20
#define ARGV_MIN_SUBS_ID 21
#define ARGV_MAX_SUBS_ID 22
#define ARGV_UNIFORM 23
#define ARGV_NUM_CLIENT_THREADS 24
#define ARGV_CLIENT_PROCESS_ID 25
#define ARGV_REPORT_TPS 26
#define ARGV_DETAILED_STATISTICS 27
/* number of static arguments that are passed to client module */
#define CLIENT_STATIC_ARGC 28
#define STATISTICS_STATIC_ARGC 8
#endif /* CONST_H */
