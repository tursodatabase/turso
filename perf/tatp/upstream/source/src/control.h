/**********************************************************************\
**  source       * control.h
**  description  * The control module is, as the name
**		           suggests, the control of the TATP benchmark.
**
**		          'main control' is in responsible of
**		          - handling of the following input files:
**		          tatp.ini, targetDBInit file,
**	    	      targetDBSchema file, DDF/TDF files, remote
**		          node info file and target database
**		          configuration file
**		          - target database schema initialization and
**		          population
**		          - benchmark session/run initializations
**		          to TIRDB
**		          - statistics and clients module invokes
**		          - communication with remote controls
**		          - starting the clients in the 'main' machine
**		          - TIRDB data finalizing and overall status reporting
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public 
**  License 1.0 as published by the Open Source Initiative (OSI). 
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifndef CONTROL_H
#define CONTROL_H

#include "communication.h"

#ifdef WIN32
#include <windows.h>
#else
/* In case of other operating systems, use OS specifiec directives
(specified in the makefile) and an OS specific header */
#include "linuxspec.h"
/* client pid table for killing the client processes */
pid_t *client_pid;
#endif /* WIN32 */

#include "tatp.h"
#include "util.h"
#include "const.h"
#include "timer.h"

/* Functions to return column values for TATP tables according
to TATP specific rules */
#include "columnvalues.h"

#define MAX_CONFIGURATION_FILE_LENGTH 64000

/* Max number of benchmarks that can be defined in one TDF */
#define MAX_BM 256
/* Maximum number of transactions in one transaction mixture */
#define MAX_NUM_OF_TRANSACTIONS 32
/* Maximum number of transaction mixes defined in one TDF */
#define MAX_NUM_OF_TRANSACTION_MIXES 16
/* Maximum number of db client distributions defined in one TDF */
#define MAX_NUM_OF_CLIENT_DISTRIBUTIONS 32

/* Default method of waiting DBMS startup */
/* -1 is polling */
#define DEFAULT_ACCELERATOR_WAIT_TIME -1

/* Default values for the run parameters */
#define DEFAULT_NUM_OF_SUBSCRIBERS 100000
#define DEFAULT_SERIAL_KEY_MODE 0            /* populate random order */
#define DEFAULT_COMMIT_BLOCK_SIZE 2000       /* rows */
#define DEFAULT_POST_POPULATION_DELAY 10     /* minutes */
#define DEFAULT_WARM_UP_DURATION 10          /* minutes */
#define DEFAULT_RUN_DURATION 20              /* minutes */
#define DEFAULT_UNIFORM 0                    /* non-uniform distribution */
#define DEFAULT_THROUGHPUT_RESOLUTION 1      /* seconds */
#define DEFAULT_CHECK_TARGETDB 0
#define DEFAULT_CLIENT_PROCESSES 1

#define DEFAULT_CLIENTDIR_PREFIX "workdir"

/* Control module modes */
enum control_modes {
    MODE_MAIN_CONTROL,
    MODE_REMOTE_CONTROL,
    MODE_REMOTE_CONTROL_PORT_SPECIFIED
};

/* A TDF content is divided to the following sections */
enum tdf_file_sections {
	NONE,
	SESSION_PARAMETERS,
	POPULATION_PARAMETERS,
	TEST_PARAMETERS,
	TRANSACTION_MIXES,
	DATABASE_CLIENT_DISTRIBUTIONS,
	TEST_SEQUENCE
};

/* Session/db-specific data from the DDF file */
struct ddfs {
        char db_name[W];
        char db_version[W];
        char db_connect[W_L];
        char os_name[W];
        char os_version[W];
        char hardware_id[W];
        char configuration_file_name[W_L];
        /* the user given name for the configuration. */
        char configuration_code[W];
        char configuration_file_contents[MAX_CONFIGURATION_FILE_LENGTH];
        /* the checksum (hash code) is counted
           based on the content of the configuration file */
        char configuration_content_checksum[W];
        char configuration_comments[W_EL];
        char db_schemafile[FILENAME_LENGTH];
        char db_initfile[FILENAME_LENGTH];
        char db_connect_initfile[FILENAME_LENGTH];
        char db_transactionfile[FILENAME_LENGTH];
        char db_schemaname[W];
};

/* A transaction probability structure */
struct transaction_prob {
        /* name of transaction definition in transaction
           definition file */
        char transact[W_L];
        /* probability for the transaction above */
        int prob;
};

/* A transactiuon mixture structure */
struct transaction_mix {
        /* name of the transaction mixture given in a TDF file */
        char name[W];
        /* Transaction propability distribution */
        struct transaction_prob tr_props[MAX_NUM_OF_TRANSACTIONS];
};

/* A remote machine client load structure */
struct remote_load {
        /* identifies the remote by indexing the structure
           'remControls' defined in remcontrol.h */
        int remControls_index;
        /* Number of clients run in the remote machine */
        int remLoad;
        int remLoadProcesses;
        /* Min and max subscriber ID*/
        int min_subs_id;
        int max_subs_id;
};

/* The remote machines database client distribution structure */
struct db_client_distribution {
        /* name of the database client distribution given
           in a TDF file */
        char name[W];
        int localLoad; /* Number of local clients */
        int localLoadProcesses;
        /* Min and max subscriber ID for local clients */
        int min_subs_id;
        int max_subs_id;
        struct remote_load rem_loads[MAX_NUM_OF_REMOTE_COMPUTERS];
};

/* The TDF data structure */
struct tdfs {
        /* The key value for the TIRDB table test_sessions */
        int session_id;
        /* For TIRDB table test_sessions */
        DATE_STRUCT start_date;
        TIME_STRUCT start_time;
        DATE_STRUCT stop_date;
        TIME_STRUCT stop_time;
        
        char control_host[W_L];
        char statistics_host[W_L];
        
        /* --Session parameters */
        char session_name[W_L];
        char author[W];
        /* the file containing the transactions run against
           the target database */
        char comments[W_L];
        /* --Population parameters */
        int subscribers;
        /* The subscribers are populated in serial order or
           in random order (a flag) */
        int serial_keys;
        int commitblock_size;
        int post_population_delay;
        int check_targetdb;
        /* --Test parameters */
        int warm_up_duration;
        int run_duration;
        int uniform;
        int throughput_resolution;        
        /* --Transaction mixes */
        struct transaction_mix tr_mixes[MAX_NUM_OF_TRANSACTION_MIXES];
        int num_of_tr_mixes;
        /* --Load distributions */
        struct db_client_distribution
		client_distributions[MAX_NUM_OF_CLIENT_DISTRIBUTIONS];
        int num_of_client_distributions;
        int repeats;
};

/* Benchmark run data. A record of this type is created for each
   test run defined in the [Test sequnce] section (including the
   'populate' commands) */
struct bmr {
        /* Key for TIRDB tables test_runs and
           transaction_mixes etc. */
        int test_run_id;
        char test_run_name[W_L];
        
        /* Either 'populate' or 'run' or 'executesql' or 'run_dedicated' */
        enum test_sequence_cmd_type cmd_type;
        
        /* For TIRDB table test_runs */
        DATE_STRUCT start_date;
        TIME_STRUCT start_time;
        DATE_STRUCT stop_date;
        TIME_STRUCT stop_time;
        
        /* --Population parameters */
        int subscribers;
        /* The subscribers are populated in serial order or
           in random order (a flag) */
        int serial_keys;
        int commitblock_size;
        int post_population_delay;
        
        int min_subscriber_id;
        
        /* --Test parameters */
        int warm_up_duration;
        int run_duration;
        int repeats;

        /* Transaction mix name (the same information is in two
           format to easy up the programming) */
        char transaction_mix_str[W_L];
        /* Transaction mix index in the trasactions structure */
        int transaction_mix_ind;
        /* Database client distribution name (the same information
           is in two format to easy up the programming)*/
        char client_distribution_str[W_L];
        /* Database client distribution index in the clients structure */
        int client_distribution_ind;
        
        /* The result of the benchmark run as the average mqth value
           (given by the Statistics module when logging off) */
        int avg_mqth;
        /* SQL file */
        char sql_file[W_L];
};

/* The set of parameters needed when starting the clients processes in 
   a participating machine */
struct clientStartParameters {
        char workDir[W_L];           /* working directory for the client process */
        int firstClient;             /* Ordinal number of the first client */
        int numOfClients;     	     /* Number of client threads to run */
        int numOfProcesses;          /* Number of client processes to run */
        char transaction_file[W_L];  /* Transaction file name */
        char db_connect[W_L];	     /* Target database odbc connect string */
        int rampup;		             /* Rampup time */
        int rampupPlusLimit;	     /* Rampup + test time */
        int verbose;		         /* Verbosity level */
        char statistics_host[W];     /* Machine address where the statistics 
                                        process is located */
        int testRunId;		         /* Test run identifier from TIRDB */
        int population_size;	     /* Number of subscribers */
        int min_subs_id;             /* Min used subscriber id */
        int max_subs_id;             /* Max used subscriber id */
        int uniform;

        int serial_keys;
        int commitblock_size;
        char db_schemafilename[FILENAME_LENGTH];
        int check_targetdb;
        
        int tr_amount;		         /* Number of transactions defined */
        int throughput_resolution;   /* Throughput resolution of the test */
        char namesAndProbs[W_EL];    /* The names and probabilities of the 
                                        transactions */
        char db_schemaname[W];       /* Schema name */
        cmd_type operation_mode;     /* Command type for client 
                                        [POPULATE / RUN] */
        int reportTPS;               /* TPS table [on/off] */
        int detailedStatistics;      /* more detailed statistics 
                                        shown in client [on/off]*/
        int waitDatabaseStart;         
        char connection_init_file[W_L];
};

/* The main function */
int main(int argc, char *argv[]);

/* Parse the command line options */
int parseOptions(int argc, char *argv[],int *ddfcount,
                 int *tdfcount, char *ddffilename, char *inifilename,
                 int *addMissing, int *dedicated, char *testSequence);

int parseParameter(char *argv);

/* The 'main control' main loop */
void mainControl(int argc, char *argv[], int ddfcount,
                 int tdfcount, char *ddffilename, char *inifilename,
                 int addMissing, int dedicatedThreads, char *testSequence);

/* Reads the tatp.ini file */
int readINI(char *workDir, char *iniFileName,
            char *RemNodsFileName, char *TIRDBConnectString,
            char *resultFileName, int *clientSynchThreshold,
            int *waitDatabaseStart);

/* Initializes the socket communication system */
int initComm();
/* Processes the Remote Nodes file */
int readRemNodsFile(char *RemNodsFileName);
/* Processes the DDF file (only one) */
int readDDF(char *ddffilename, struct ddfs *ddf);
/* Checks if the parameters for TIRDB access are present */
int checkDDFparameters(const struct ddfs *ddf);
/* Processes given TDF files (may be several of them) */
int ctrlTDF(struct ddfs *ddf, char *workDir, char *TIRDBConnectString,
            char *resultFile, char *tdfname, int clientSynchThreshold,
            int waitDatabaseStart, int addMissing, int dedicatedThreads,
            char *testSequence);

/* Processes one TDF file */
int readTDF(char *tdffilename, struct tdfs *tdf, struct bmr *bmrs[], 
            int *num_of_bmr, char *testSequence);

/* Initialize the TDF and DDF data structures */
void initTDFDataStruct(struct tdfs *tdf, int defaultvalues);
void initDDFDataStruct(struct ddfs *ddf);

/* Check the validity of TDF data */
int checkTDFData(struct tdfs *tdf, struct bmr *bmrs[], int num_of_bmr, 
                 char *tdffilename);

/* Checks wherher the given line contains a TDF section marker */
int isTDFSectionMarker(const char *line,
                       enum tdf_file_sections *tdf_file_section);
/* Parses a session parameter from the tdf file */
int parseTDFSessionParameter(const char *line, struct tdfs *tdf);
/* Parses a population parameter from the tdf file */
int parseTDFPopulationParameter(const char *line, struct tdfs *tdf);
/* Parses a test parameter from the tdf file */
int parseTDFTestParameter(const char *line, struct tdfs *tdf);
/* Parses a transaction mix line from the tdf file */
int parseTDFTransactionMixes(const char *line, struct tdfs *tdf,
                             int *parsing_transaction_mix, 
                             int *transaction_num);

/* Parses a load distribution line from the tdf file */
int parseTDFLoadDistributions(const char *line, struct tdfs *tdf,
                              int *parsing_client_distribution,
                              int *client_num);

/* Parses a test sequence from the tdf file */
int parseTDFTestSequence(char *line, struct tdfs *tdf, struct bmr *bm_run[], 
                         int *num_of_bmr, int *repeats);

/* Initializes the parameter values for a benchmark run data structure */
void initBMRunParameters(struct tdfs *tdf, struct bmr *bm_run);
/* Digests basic operation type from a [Test sequece] command of TDF */
int digestBasicOperationType(char *line,
                             enum test_sequence_cmd_type *operationType);

/* Checks if the remote was defined in the remoteNodes file */
int isRemoteDefined(const char* remoteName);
/* Checks that certain connections to remote controls are established */
int checkRemoteConnections(struct tdfs *tdf, const struct bmr *bmr);
/* Checks that certain remote controls are responding */
int pingRemotes(struct tdfs *tdf, const struct bmr *bmr);
/* Waits for the OK message from all the remotes and local clients */
int getClientResponses(struct tdfs *tdf, struct bmr *bmrs, 
                       int waitDatabaseStart);

/* Propagates the test time to the remotes. Counts the synch. delay
based on the receive time of the return messges */
int propagateTestTime(struct timertype_t *testTimer,
                      int clientSynchThreshold, struct tdfs *tdf,
                      struct bmr *bmrs);

/* Gives the test time to local clients. Counts the synch. delay
based on the receive time of the return messges */
int testTimeToLocalClients(struct timertype_t *testTimer,
                           int clientSynchThreshold, int numOfLocalClients);

/* Sends a 'start test' messages to all the clients */
int startTest(struct tdfs *tdf, struct bmr *bmrs);
/* Sends a 'interrupr test' messages to all the clients */
int interruptTest(struct tdfs *tdf, struct bmr *bmrs);
/* Collects test session logs and saves them in the specific
directory on main control node */
int collectTestRunLogs(char *workDirBase, struct tdfs *tdf, const struct bmr *bmrs);
void archiveTestSessionLogs(struct tdfs *tdf, int after_run);

/* Receives a file content from the communication port.
Stores the file in the main directory. */
int receiveFile(const char* dataFragment, const char* path);

/* Functions that spawn and control other processes
('client' and 'statistics' modules) */
int spawnStatistics(struct tdfs *tdf, const struct bmr *bmr, int storeResults,
                    char *TIRDBConnectString, char *resultFileName);

int spawnClientsInNetwork(struct ddfs *ddf, struct tdfs *tdf,
                          const struct bmr *bmr, char *workDirBase, int *mainClients,
                          char *DBSchemaName, int waitDatabaseStart);

/* Send the spawn clients messages to all the remotes */
int sendSpawnClientMessages(struct tdfs *tdf, const struct bmr *bmrs);
int spawnClients(/*const*/ struct clientStartParameters* csp);
void spawnError();

/* Clean up client processes */
int finalizeTestInNetwork(struct tdfs *tdf, const struct bmr *bmr,
                          int mainClients);
int cleanUpClients(int clients);

/* Functions that handle the database product configuration */
int readConfigurationFile(struct ddfs *ddf);

int finalize();

/* Helper functions */
int waitStatisticsMessage(struct bmr *bmrs);
void setDateTimeNow(DATE_STRUCT *d, TIME_STRUCT *t);
void freeBenchmarks(struct bmr *bmrs[], int num_of_bmrs);

#endif /* CONTROL_H */
