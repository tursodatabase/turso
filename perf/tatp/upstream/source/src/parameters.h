#ifndef PARAMETERS_H
#define PARAMETERS_H

#include "const.h"
#include "communication.h"
#include "util.h"

#define SIMPLEMATCH_FILL_STR(target, r, line, start, length, pattern, limit){ \
        if (!simpleMatch(r, line, pattern, &start, &length)) {               \
            if (target != NULL) {                                            \
                strncpy(target, &line[start], minimum(limit, length+1));     \
                target[length] = 0;                                          \
            }                                                                \
            continue;                                                        \
        }                                                                    \
}


#define SIMPLEMATCH_FILL_INT(target, r, line, start, length, pattern) {      \
        if (!simpleMatch(r, line, pattern, &start, &length)) {               \
            if (target != NULL) {                                            \
                strncpy(tempStr, &line[start], minimum(W_L, length+1));      \
                tempStr[length] = 0;                                         \
            }                                                                \
            *target = atoi(tempStr);                                         \
            continue;                                                        \
        }                                                                    \
}

/* Default values for the run parameters */
#define DEFAULT_NUM_OF_SUBSCRIBERS 100000
#define DEFAULT_SERIAL_KEY_MODE 0            /* populate random order */
#define DEFAULT_COMMIT_BLOCK_SIZE 2000       /* rows */
#define DEFAULT_POST_POPULATION_DELAY 10     /* minutes */
#define DEFAULT_WARM_UP_DURATION 10          /* minutes */
#define DEFAULT_RUN_DURATION 20              /* minutes */
#define DEFAULT_UNIFORM 0                    /* non-uniform distribution */
#define DEFAULT_THROUGHPUT_RESOLUTION 1      /* seconds */
#define DEFAULT_CLIENT_PROCESSES 1

/* Max number of benchmarks that can be defined in one TDF */
#define MAX_BM 256
/* Maximum number of transactions in one transaction mixture */
#define MAX_NUM_OF_TRANSACTIONS 32
/* Maximum number of transaction mixes defined in one TDF */
#define MAX_NUM_OF_TRANSACTION_MIXES 16
/* Maximum number of db client distributions defined in one TDF */
#define MAX_NUM_OF_CLIENT_DISTRIBUTIONS 16

#define MAX_CONFIGURATION_FILE_LENGTH 64000

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
        /* the file containing the transactions run against
           the target database */
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
           'remControls' */
        int remControls_index;
        /* Number of clients run in the remote machine */
        int remLoad;
        int remLoadProcesses;
        /* Min and max subscriber ID */
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
        
        /* Either 'populate' or 'run' or 'executesql' */
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

/* Initializes the parameter values for a benchmark run data structure */
void initBMRunParameters(struct tdfs *tdf, struct bmr *bm_run);

/* Digests basic operation type from a [Test sequece] command of TDF */
int digestBasicOperationType(char *line,
                             enum test_sequence_cmd_type *operationType);
/* Initializes the TDF data structure */
void initTDFDataStruct(struct tdfs *tdf);
/* Check the validity of TDF data */
int checkTDFData(struct tdfs *tdf, struct bmr *bmrs[], int num_of_bmr, 
                 char *tdffilename);
/* Checks if the parameters for TIRDB access are present */
int checkDDFparameters(const struct ddfs *ddf);
/* Processes the Remote Nodes file */
int readRemNodsFile(char *RemNodsFileName);
/* Processes the DDF file (only one) */
int readDDF(char *ddffilename, struct ddfs *ddf);
/* Processes one TDF file */
int readTDF(char *tdffilename, struct tdfs *tdf, struct bmr *bmrs[], 
            int *num_of_bmr);
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

#endif /* PARAMETERS_H */

