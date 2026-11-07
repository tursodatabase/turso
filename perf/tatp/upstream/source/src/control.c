/**********************************************************************\
**  source       * control.c
**  description  * The control module is, as the name
**		           suggests, the control of the TATP benchmark. The
**                 control may be run either as the 'main control' or as
**                 a 'remote control' (command option -r)
**
**		         * 'main control' is in responsible of
**		            - handling of the following input files:
**					  INI file, target DB initialization file,
**					  target DB schema file, DDF/TDF files,
**                    remote node info file and target database
**					  configuration file
**		            - target database schema initialization and
**					  population
**		            - benchmark session/run initializations to TIRDB
**		            - statistics and clients module invokes
**			        - communication with remote controls
**			        - starting the clients in the 'main' machine
**		            - TIRDB data finalizing and overall status
**					  reporting
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#include <string.h>
#include <stdlib.h>

#ifdef WIN32
#include <process.h>
#include <direct.h>
#else
/* In case of other operating systems, use OS specific directives
	(specified in the MAKE file) and an OS specific header */
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>
#ifndef __hpux
#include <spawn.h>
#endif
#endif /* WIN32 */

#include <errno.h>
#include <time.h>
#include "const.h"
#include "tatpversion.h"
#include "control.h"
#include "remcontrol.h"
#include "tirdb.h"
#include "random.h"

extern char** environ;

#ifndef WIN32
#define CLIENT_BINARY_NAME "client"
#define STATISTICS_BINARY_NAME "statistics"
#else
#define CLIENT_BINARY_NAME "client.exe"
#define STATISTICS_BINARY_NAME "statistics.exe"
#endif

#define WAIT_S_RETRIES 10

/* Size of the population will be stored here. It is
	made global for practical reasons */
int popl_size = 0;

static log_t g_log;
communication_t g_comm;

struct server_t* server = NULL;

/* test parameters that will overrule the parameters given in TDF or DDF */
static struct tdfs* tdf_cmdline = NULL;
static struct ddfs* ddf_cmdline = NULL;

#ifndef WIN32
    pid_t statistics_pid = 0;       /* Process ID */
#else
    intptr_t statistics_pid = 0;    /* Process ID */
#endif

/* indicates the mode in which the Control is run. That is
either 'Main Control' or 'Remote Control' */
int controlModuleMode;
/* listener port */
int controlModulePortNumber = MAIN_CONTROL_PORT;

/* Lookup table for CRC (checksum) calculations */
ULONG CRC32LookupTable[256];

/* whether TPS table should be used or not */
int reportTPS = 0;
/* more statistics */
int showDetailedStatistics = 0;

/* absolute path to the directory where binaries are located */
char strProgramDir[W_L];

/*##**********************************************************************\
 *
 *      getLogObject
 *
 * Returns a pointer to log object
 *
 * Parameters:
 *      none
 * Return value
 *      Pointer to log object
 */
log_t* getLogObject(void)
{
    return &g_log;
}

/*##**********************************************************************\
 *
 *      main
 *
 * Entry point of the TATP benchmark suite. Either 'mainControl'
 * or 'remoteControl' is called depending on the run mode (defined
 * with the option -r).
 *
 * Parameters :
 *      argc
 *          Argument count from the command line
 *      argv
 *          Argument values from the command line
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int main(int argc, char *argv[])
{
        /* Data Definition file name (from the command line) */
        char    ddffilename[FILENAME_LENGTH] = {'\0'};
        /* INI file name */
        char    inifilename[FILENAME_LENGTH] = DEFAULT_INIFILE_NAME;

        /* test Sequence for cmd line operation */
        char    testSequence[W_L] = {'\0'};

        FILE    *fTest = NULL;
        char    msg[80];
        int     tdfcount = 0;
        int     ddfcount = 0;
        int     i;
        int     addMissingTIRDBvalues = 0;
        int     dedicatedThreads = 0;

#ifndef WIN32
        struct sigaction    sig_pipe;

/* Defining SIGPIPE handler - useful because cmm listener closes
 * the pipe between main and it, thus main will receive SIGPIPE.
 */
        memset(&sig_pipe, 0, sizeof(sig_pipe) );
        sig_pipe.sa_flags = SA_SIGINFO;
        sig_pipe.sa_handler = SIG_IGN;

/* Set the signal handlers */
        if (sigaction(SIGPIPE, &sig_pipe, NULL) == -1) {
            sprintf(msg, "main : Setting signal handler for SIGPIPE failed."
                    "Errno = %d. Exiting.\n", errno);
            message('F', msg);
            exit(E_FATAL);
        }
#endif /* WIN32 */

        /* Initialize the globals of the logging system.
           Defined in util.h. Default logging level is all
           but debug messages */
        initializeLog(DEFAULT_VERBOSITY_LEVEL, "CONTROL", 2);

        /* The control is run as the 'main control' by default
        (without the command option -r)*/
        controlModuleMode = MODE_MAIN_CONTROL;

        /* get directory from where the binary was run */
        for (i = strlen(argv[0]); i>0; --i) {
            if (argv[0][i] == '\\' || argv[0][i] == '/') {
                strncpy(strProgramDir, argv[0], i+1);
                break;
            }
        }
        if (i == 0) {
            strcpy(strProgramDir, "." PATH_SEPARATOR);
        }

        if (!chdir(strProgramDir)) {
            /* replace with absolute path */
            if (!GET_CWD(strProgramDir, W_L)) {
                writeLog('F', "Unable to get current working directory");
                return E_FATAL;
            } else {
                strcat(strProgramDir, PATH_SEPARATOR);
            }
        } else {
            writeLog('F', "Unable to change working directory");
            return E_FATAL;
        }

        /* Check that the client executable exists */
        /* for both Main and Remote Control */
        if (openFileWithPath(&fTest, CLIENT_BINARY_NAME, strProgramDir) > 0) {
            sprintf(msg, "'" CLIENT_BINARY_NAME "' program module not found, "
                    "exiting...");
            message('E', msg);
            finalize();
            exit(E_FATAL);
        }
        else {
            fclose(fTest);
        }

        /* Check that the statistics executable exists */
        /* Statistics runs in the same node with Main Control */
        if (controlModuleMode == MODE_MAIN_CONTROL) {
            if (openFileWithPath(&fTest, STATISTICS_BINARY_NAME,
                                 strProgramDir) > 0) {
                sprintf(msg, "'" STATISTICS_BINARY_NAME "' program module "
                        "not found, exiting...");
                message('E', msg);
                exit(E_FATAL);
            }
            else {
                fclose(fTest);
            }
        }

        /* Handles given command line parameters */
        /* checks that given tdf and ddf files start with valid tag */
        if (parseOptions(argc, argv, &ddfcount, &tdfcount, ddffilename,
                         inifilename, &addMissingTIRDBvalues,
                         &dedicatedThreads, testSequence) != E_OK) {
            finalize();
            exit(E_FATAL);
        }

        /* Initialize messaging system. */
        if (initComm()) {
            message('F', "Initializing TATP communications failed");
            finalize();
            exit(E_FATAL);
        }

        if (controlModuleMode == MODE_MAIN_CONTROL) {
            /* Execute TDF files one by one */
            mainControl(argc, argv, ddfcount, tdfcount, ddffilename,
                        inifilename, addMissingTIRDBvalues, dedicatedThreads,
                        testSequence);
        } else {
            /* Run as 'remote control'. It runs for ever... */
            remoteControl();
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      parseParameter
 *
 * Handle a parameter given with -x option
 *
 *      argv
 *          parameter string from the command line
 *
 * Return value:
 *      E_OK - success
 *      E_FATAL - fatal error
 */
int parseParameter(char *argv) {
        char msg[2*W_L];
        char *param;
        regex_t *r;
        int err = 0, count = 0, starts[100], lengths[100];
        int ret = E_OK;

        /* initialize regexp parser */
        r = (regex_t *) malloc(sizeof(regex_t));
        if (r == NULL) {
            writeLog('F', "Cannot reserve memory for regex_t");
            return E_FATAL;
        }

        param = (char*)malloc(sizeof(char)*(strlen(argv)+1));
        strcpy(param, argv);

        if (tdf_cmdline == NULL) {
            /* allocate storage for cmdline parameters */
            tdf_cmdline = (struct tdfs*) malloc(sizeof(struct tdfs));
            initTDFDataStruct(tdf_cmdline, 0);
        }
        if (ddf_cmdline == NULL) {
            /* allocate storage for cmdline parameters */
            ddf_cmdline = (struct ddfs*) malloc(sizeof(struct ddfs));
            initDDFDataStruct(ddf_cmdline);
        }

        if (!multiMatch(r,
                        argv,
                        "^(.*) *= *(.*) *$",
                        &count,
                        starts,
                        lengths)) {
            err = extractIntKeyword(param, "subscribers",
                                    &(tdf_cmdline->subscribers));
            if (err)
                err = extractIntKeyword(param, "warm_up_duration",
                                        &(tdf_cmdline->warm_up_duration));
            if (err)
                err  = extractIntKeyword(param, "run_duration",
                                         &(tdf_cmdline->run_duration));
            if (err)
                err  = extractIntKeyword(param, "repeats",
                                         &(tdf_cmdline->repeats));
            if (err)
                err = extractStringKeyword(param, "transaction_file",
                                           ddf_cmdline->db_transactionfile,
                                           FILENAME_LENGTH);
            if (err)
                err = extractStringKeyword(param, "targetdbschema",
                                           ddf_cmdline->db_schemafile,
                                           FILENAME_LENGTH);
            if (err)
                err = extractStringKeyword(param, "db_connect",
                                           ddf_cmdline->db_connect, W_L);
            if (err)
                err = extractStringKeyword(param, "transaction_mix",
                                           (tdf_cmdline->tr_mixes[0]).name, W);
            if (err)
                err = extractStringKeyword(param, "database_client_distribution",
                                           (tdf_cmdline->client_distributions[0]).name, W);
        }
        if (err) {
            sprintf(msg, "Invalid test parameter definition: '%s'", argv);
            writeLog('E', msg);
            ret = E_ERROR;
        } else {
            sprintf(msg, "Test parameter defined: '%s'", argv);
            writeLog('I', msg);
        }
        free(param);
        free(r);
        return ret;
}

/*##**********************************************************************\
 *
 *      parseOptions
 *
 * Handles command line options. Clears the option string after it is
 * handled (exception: tdf file names are not cleared). Checks that
 * the DDF and TDF files given in the command line exist and that the
 * tags starting the files are correct (main control).
 *
 * Parameters:
 *      argc
 *          Argument count
 *
 *      argv
 *          Argument values
 *
 *      ddfcount
 *          Pointer to the count of DDF file names encountered
 *
 *      tdfcount
 *          Pointer to the count of TDF file names encountered
 *
 *      ddffilename
 *			Name of the DDF file as the function returns
 *
 *      inifilename
 *          Pointer to string to store the INI file name
 *
 *      addMissing
 *          Pointer to int for 'add missing TIRDB values' boolean
 *
 * Return value:
 *      E_OK - success
 *      E_FATAL - fatal error
 */
int parseOptions(int argc, char *argv[], int *ddfcount,
                 int *tdfcount, char *ddffilename, char *inifilename,
                 int *addMissing, int *dedicatedThreads, char *testSequence)
{
        char    msg[2*W_L];
        char    *opt = NULL;
        int     ret = E_OK;
        int     err = 0;
        FILE    *file = NULL;
        /* A file line */
        char    line[1024];

        ret = 0;
        *ddfcount = 0;
        *tdfcount = 0;
        /* Iterate through the command arguments */

        while (--argc > 0) {
            argv++;
            if (**argv == '-') { /* an option encountered */
                opt = *argv;
                switch (*++opt) {
                    case 'h':
                        printf("Usage: tatp [options] ddf_file tdf_file\n\n");
                        printf("Options valid when run as Main Control process (default):\n");
                        printf("  -a               automatically add missing values to TIRDB\n");
                        printf("  -c path          set working directory to <path>\n");
                        printf("  -d               run transactions in dedicated individual threads\n");
                        printf("                   (ignoring transaction mix percentages and client distribution settings) \n");
                        printf("  -e command       execute a single Test sequence <command>\n");
                        printf("  -h               print the usage instructions\n");
                        printf("  -i filename      set INI file to <filename>, default is '" DEFAULT_INIFILE_NAME "'\n");
                        printf("  -s               show more detailed statistics after a test run\n");
                        printf("  -t               enable online TPS monitoring\n");
                        sprintf(msg,"  -vX              set verbosity level (X = [1-5]), default is %d\n", DEFAULT_VERBOSITY_LEVEL);
                        printf(msg);
                        printf("  -x param=value   set parameter value which replaces the default value and \n");
                        printf("                   also overrules the values given in DDF and TDF\n\n");
                        printf("Options valid when run as Remote Control process:\n");
                        printf("  -r               run in Remote Control mode \n");
                        printf("  -p <portnumber>  set Remote Control listener port to <portnumber>, needs '-r' to be already given\n\n");
                        sprintf(msg, "TATP version: %s\n\n", TATPVERSION);
                        printf(msg);
                        return E_NOT_OK;
                        break;
                    case 'e':
                        /* next argument is the Test sequence */
                        **argv = '\0';
                        if ((argc - 1) > 0) {
                            argv++;
                            argc--;
                            if (strlen(*argv) > 1) {
                                strncpy(testSequence, *argv, W_L);
                            }
                        }
                        break;
                    case 'a':
                        *addMissing = 1;
                        break;
                    case 'v':
                        switch (*++opt)
                        {
                            case '0':
                                g_log.verbose = 0;
                                break;
                            case '1':
                                g_log.verbose = 1;
                                break;
                            case '2':
                                g_log.verbose = 2;
                                break;
                            case '3':
                                g_log.verbose = 3;
                                break;
                            case '4':
                                g_log.verbose = 4;
                                break;
                            case '5':
                                g_log.verbose = 5;
                                break;
                            case '6':
                                g_log.verbose = 6;
                                break;
                            case '\0':
                                writeLog('F', "Missing verbosity level");
                                ret = E_FATAL;
                                break;
                            default:
                                sprintf(msg, "Unknown verbosity level '%c'",
                                        *opt);
                                writeLog('F', msg);
                                ret = E_FATAL;
                        }
                        sprintf(msg, "Verbosity level is: %d", g_log.verbose);
                        writeLog('I', msg);
                        break;
                    case 'r':
                        /* the remote node mode */
                        controlModuleMode = MODE_REMOTE_CONTROL;
                        break;
                    case 'p':
                        /* set listener port for remote node */
                        **argv = '\0';
                        if ((argc - 1) > 0) {
                            argv++;
                            argc--;
                            if (controlModuleMode == MODE_REMOTE_CONTROL) {
                                if ((controlModulePortNumber = atoi(*argv)
                                        ) > 0) {
                                    controlModuleMode
                                        = MODE_REMOTE_CONTROL_PORT_SPECIFIED;
                                    sprintf(msg, "Remote Control TCP "
                                            "listening port set to: %d",
                                            controlModulePortNumber);
                                    writeLog('I', msg);
                                } else {
                                    sprintf(msg, "Invalid port number "
                                            "'%s' given in command line.",
                                            *argv);
                                    writeLog('F', msg);
                                    ret = E_FATAL;
                                }
                            } else {
                                writeLog('F', "Please use '-r' argument "
                                         "before giving Remote Control "
                                         "listening port number with '-p'.");
                                ret = E_FATAL;
                            }
                        }
                        break;
                    case 's':
                        showDetailedStatistics = 1;
                        break;
                    case 't':
                        reportTPS = 1;
                        break;
                    case 'i':
                        /* next argument is filename */
                        **argv = '\0';
                        if ((argc - 1) > 0) {
                            argv++;
                            argc--;
                            if (strlen(*argv) > 1) {
                                strncpy(inifilename, *argv, FILENAME_LENGTH);
                                sprintf(msg, "Using INI file '%s'", *argv);
                                writeLog('I', msg);
                            }
                        }
                        break;
                    case 'c':
                        /* next argument is working directory */
                        **argv = '\0';
                        if ((argc - 1) > 0) {
                            argv++;
                            argc--;
                            if (strlen(*argv) > 1) {
                                err = CH_CWD(*argv);
                                if (err) {
                                    sprintf(msg, "Unable to set working "
                                            "directory to: ");
                                    strncat(msg, *argv, 2*W_L-strlen(msg)-1);
                                    writeLog('F', msg);
                                    return E_FATAL;
                                } else {
                                    sprintf(msg, "TATP working directory "
                                            "set to: ");
                                    strncat(msg, *argv, 2*W_L-strlen(msg)-1);
                                    writeLog('I', msg);
                                }
                            }
                        }
                        break;
                    case 'd':
                        *dedicatedThreads = 1;
                        break;
                    case 'x':
                        /* next argument is parameter */
                        **argv = '\0';
                        if ((argc - 1) > 0) {
                            argv++;
                            argc--;
                            if (strlen(*argv) > 1) {
                                if (parseParameter(*argv)) {
                                    /* unrecognized parameter or illegal value */
                                    return E_FATAL;
                                }
                            }
                        }
                        break;
                    default:
                        /* The option given was not a TATP option */
                        sprintf(msg, "Unknown option -%c", *opt);
                        writeLog('F', msg);
                        ret = E_FATAL;
                }
                **argv = '\0'; /* Option handled. Empty it. */
            }
            else {
                /* has to be a DDF or TDF file name */
                if (openFile(&file, *argv) != 0) {
                    sprintf(msg, "Cannot open file '%s'", *argv);
                    message('F', msg);
                    return E_FATAL;
                }
                if (readFileLine(file, line, 256) == -1) {
                    sprintf(msg, "File '%s' is empty", *argv);
                    message('F', msg);
                    return E_FATAL;
                }
                /* check the file type from the first line of the file */
                if (strncmp(line, "//tatp_ddf", 10) == 0) {
                    /* we can only have one DDF file */
                    strncpy(ddffilename, *argv, FILENAME_LENGTH);
                    (*ddfcount)++;
                    message('D', "DDF file found");
                    **argv = '\0'; /* Option handled. Empty it */
                }
                else if (strncmp(line, "//tatp_tdf", 10) == 0) {
                    (*tdfcount)++;
                    message('D', "TDF file found");
                }
                else {
                    sprintf(msg, "Parameter '%s' is not a DDF nor "
                            "a TDF file.", *argv);
                    message('F', msg);
                    return E_FATAL;
                }
                fclose(file);
            }
        }
        return ret;
}

/*##**********************************************************************\
 *
 *      mainControl
 *
 * The main control function. The TDF files given in the command line
 * are processed in the main loop (main control).
 *
 * Parameters:
 *      argc
 *		    number of command line arguments
 *
 *      argv
 *          command line arguments (only TDF file names)
 *          others have been emptied
 *
 *      ddfcount
 *          number of DDF files given in the command line
 *
 *      tdfcount
 *          number of TDF files given in the command line
 *
 *      ddffilename
 *          TDF file name
 *
 *      iniFileName
 *          INI file name
 *
 *      addMissing
 *          'Add missing values to TIRDB' boolean
 *
 *      dedicatedThreads
 *          is set, client process is instructed to run transactions
 *          at full speed in dedicated threads (ignoring transaction mix and
 *          client distribution)
 *
 *      testSequence
 *          if not empty string, the test sequence command that replaces the
 *          sequence in TDF
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
void mainControl(int argc, char *argv[], int ddfcount, int tdfcount,
                 char *ddffilename, char *iniFileName, int addMissing,
                 int dedicatedThreads, char *testSequence) {
        char TIRDBConnectString[W_L];
        char resultFileName[W_L];
        /* Client synch threshold value in milliseconds, read from tm.ini,
           default used if not defined in the file */
        int     clientSynchThreshold;
        /* Remote nodes file name */
        char    RemNodsFileName[FILENAME_LENGTH];
        /* wait time for accelerator to start */
        int waitDatabaseStart = DEFAULT_ACCELERATOR_WAIT_TIME;

        /* client working directory */
        char workDir[W_L] = {'\0'};

        /* the data definition file structure */
        struct  ddfs ddf;
        char    msg[80];
        int     err = 0;
        int     errs = 0;
        int     i;
#ifdef ACCELERATOR
#ifdef CACHE_MODE
        char    c;
#endif        
#endif
        clientSynchThreshold = DEFAULT_CLIENT_SYNCH_THRESHOLD;

        sprintf(msg, "*** Start TATP v. %s", TATPVERSION);
        message('I', msg);

        if (ddfcount == 0) {
            if ((ddf_cmdline == NULL) || (ddf_cmdline->db_connect[0] == '\0')) {
                message('F', "No DDF given in the command line or not all required DDF parameters given using -x option");
                message('F', "If the file was given as a parameter, check the first line (should be //tatp_ddf)");
                finalize();
                exit(E_FATAL);
            }
        }
        if (ddfcount > 1) {
            message('F', "More than one DDF given in the command line");
            finalize();
            exit(E_FATAL);
        }
        if (tdfcount == 0) {
            message('F', "No TDF given in the command line");
            message('F', "If one was given, check first line (//tatp_tdf)");
            finalize();
            exit(E_FATAL);
        }
        /* Initialize the CRC32 checksum lookup table */
        CRC32InitTable(CRC32LookupTable);
        InitRemotesStruct();

        /* Read TATP initialization file. */
        /* Sets TIRDBConnectString. Checks also that the database
           initialization and schema files exist if they are defined in
           tatp.ini */
        if (readINI(workDir,
                    iniFileName,
                    RemNodsFileName,
                    TIRDBConnectString,
                    resultFileName,
                    &clientSynchThreshold,
                    &waitDatabaseStart)) {
            message('F', "Initializing TATP failed");
            finalize();
            exit(E_FATAL);
        }

        /* Read the remote nodes file (if it was defined) */
        if (*RemNodsFileName != '\0') {
            err = readRemNodsFile(RemNodsFileName);
            if (err == E_FATAL) {
                sprintf(msg, "Fatal error in reading Remote Nodes file '%s'",
                        RemNodsFileName);
                message('F', msg);
                finalize();
                exit(E_FATAL);
            }
        }

        /* Clear ddf data */
        initDDFDataStruct(&ddf);

        /* Read the data definition file (DDF) */
        if (*ddffilename != '\0') {
            err = readDDF(ddffilename, &ddf);
            if (err == E_FATAL) {
                message('F', "Fatal error in DDF read");
                finalize();
                exit(E_FATAL);
            }
        }

        /* overwrite DDF parameter values with the ones given as cmd line options */
        if (ddf_cmdline != NULL) {
            if (ddf_cmdline->db_transactionfile != '\0') {
                strcpy(ddf.db_transactionfile, ddf_cmdline->db_transactionfile);
            }
            if (ddf_cmdline->db_connect != '\0') {
                strcpy(ddf.db_connect, ddf_cmdline->db_connect);
            }
            if (ddf_cmdline->db_schemafile != '\0') {
                strcpy(ddf.db_schemafile, ddf_cmdline->db_schemafile);
            }
        }

        if (*TIRDBConnectString != '\0') {
                err = checkDDFparameters(&ddf);
                if (err == E_FATAL) {
                    message('F', "Some TIRDB parameters in DDF missing or invalid");
                    finalize();
                    exit(E_FATAL);
                }

                /* Check access to the database configuration file and
                   store it to ddf */
                if (readConfigurationFile(&ddf)) {
                    message('F',
                            "The database configuration file could not be read");
                    exit(E_FATAL); /* Fatal error */
                }
        }

        if (*(ddf.db_initfile) != '\0') {
            /* Execute the SQL commands read from the targetDBInit file
               against the target database */
            err = processSQLFile(ddf.db_initfile, NULL, &server, ddf.db_connect);
            if (err == E_FATAL) {
                message('F',
                        "Error in target database ini file processing");
                finalize();
                exit(E_FATAL);
            }
        }

        /* Iterate through the TDF files defined in the command line  */
        /* ctrTDF handles the actual processing of a TDF file */
        for (i = 1; i < argc; i++) {
            if (*argv[i] != '\0') {
                err = ctrlTDF(&ddf,
                              workDir,
                              TIRDBConnectString,
                              resultFileName,
                              argv[i],
                              clientSynchThreshold,
                              waitDatabaseStart,
                              addMissing,
                              dedicatedThreads,
                              testSequence);

                if (err == E_FATAL) {
                    message('F', "Fatal error occurred, exiting.");
                    finalize();
                    exit(E_FATAL);
                }
                if (err == E_ERROR) {
                    errs = E_ERROR;
                }
            }
        }

        /* stop local server if it is started */
        if (server != NULL) {
#ifdef ACCELERATOR
#ifdef CACHE_MODE
            /* the frontend is now running, wait for replication to finish */
            writeLog('I',
                     "Press enter when you are ready to stop the benchmark.");
            c = getchar();
#endif
#endif
            err = stopServer(server);
            if (err != 0) {
                sprintf(msg, "Could not stop database server (%s), error %d",
                        server_name,
                        err);
                message('E', msg);
            } else {
                server = NULL;
            }
        }

        /* All TDFs processed */
        /* Cleanup routines */
        finalize();

        exit(errs);
}

/*##**********************************************************************\
 *
 *      readINI
 *
 * TATP initialization file and by opening the log file and
 * initializing the communication (socket) system.  Also initializes
 * the CRC32 checksum lookup table
 *
 * Parameters:
 *      workDir
 *          Pointer for storing working directory for client processes
 *
 *      iniFileName
 *		    Pointer for storing the target database initialization file
 *
 *      RemNodsFileName
 *			Pointer for storing the remote nodes file
 *
 *      TIRDBConnectString
 *          Pointer for storing TIRDB connect string
 *
 *      resultFileName
 *          resultFile for storing results instead of TIRDB
 *
 *      clientSynchThreshold
 *			Pointer for storing client synchronization threshold value
 *
 *      waitDatabaseStart
 *          Pointer for storing waitDatabaseStart value
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int readINI(char *workDir, char *iniFileName, char *RemNodsFileName, char *TIRDBConnectString,
            char *resultFileName, int *clientSynchThreshold, int *waitDatabaseStart)
{
        FILE    *fIni;
        FILE    *fTest;
        char    line[256];
        /* A flag indicating the first line of a file */
        int     firstline;
        char    LogFileName[256];
        char    msg[300];
        char    tempStr[32];
        int     err;
        int     exitInit;
        /* The reqular expression data structure (used to
           match keywords from the initialization file) */
        regex_t *r;
        int     start;
        int     length;

        fIni = fTest = NULL;
#ifdef _DEBUG
        /* For TATP software performance analysis,
           not needed in actual benchmark runs */
        initializeTiming();
#endif /* _DEBUG */

        /* Open and handle the initialization file */
        if (openFile(&fIni, iniFileName) != 0) {
            sprintf(msg,
                    "Cannot open initialization file '%s'", iniFileName);
            writeLog('F', msg);
            return E_FATAL;
        }
        /* initialize regexp parser */
        r = (regex_t *) malloc(sizeof(regex_t));
        if (r == NULL) {
            writeLog('F', "Cannot reserve memory for regex_t");
            return E_FATAL;
        }
        firstline = 1;
        TIRDBConnectString[0] = 0;
        resultFileName[0] = '\0';

        strncpy(LogFileName,
                DEFAULT_LOG_FILE_NAME,
                strlen(DEFAULT_LOG_FILE_NAME)+1);
        RemNodsFileName[0] = 0;
        err = 0;
        exitInit = 0;

        while (readFileLine(fIni, line, 256) != -1) {
            if (err == E_ERROR) {
                /* Handle the rest of the ini file and then exit with fatal
                 * error */
                exitInit = 1;
            }
            if (firstline) {
                firstline = 0;
                if (strncmp(line, "//tatp_ini", 10) != 0) {
                    message('F',
                            "The initialization file has wrong or no "
                            "identification line");
                    message('F', "Check first line (//tatp_ini)");
                    fclose(fIni);
                    return E_FATAL;
                }
                continue;
            }
            /* Removes potential comments from the line just read */
            removeComment(line);

            /* Skip blank lines */
            if (*line == '\0') {
                continue;
            }
            writeLog('D', line);
            /* Try to find the keywords (the directives) from the line */
            if (!simpleMatch(r,
                             line,
                             "^tirdbconnect *= *\"(.*)\" *$",
                             &start,
                             &length)) {
                strncpy(TIRDBConnectString, &line[start], minimum(256,
                                                                  length+1));
                TIRDBConnectString[length] = 0;
                continue;
            }
            if (!simpleMatch(r,
                             line,
                             "^resultfile *= *\"(.*)\" *$",
                             &start,
                             &length)) {
                strncpy(resultFileName, &line[start], minimum(W_L,
                                                          length+1));
                resultFileName[length] = '\0';
                continue;
            }
            if (!simpleMatch(r,
                             line,
                             "^clientdirbase *= *\"(.*)\" *$",
                             &start,
                             &length)) {
                strncpy(workDir, &line[start], minimum(W_L,
                                                       length+1));
                workDir[length] = '\0';
                continue;
            }
            if (!simpleMatch(r,
                             line,
                             "^log *= *\"(.*)\" *$",
                             &start,
                             &length)) {
                strncpy(LogFileName, &line[start], minimum(256, length+1));
                LogFileName[length] = 0;
                continue;
            }
            if (!simpleMatch(r,
                             line,
                             "^remotenodes *= *\"(.*)\" *$",
                             &start,
                             &length)) {
                strncpy(RemNodsFileName, &line[start], minimum(256, length+1));
                RemNodsFileName[length] = 0;
                continue;
            }
            if (!simpleMatch(r,
                             line,
                             "^synchthreshold *= *(.*) *$",
                             &start,
                             &length)) {
                strncpy(tempStr, &line[start], minimum(256, length+1));
                *clientSynchThreshold = atoi(tempStr);
                continue;
            }
            if (!simpleMatch(r,
                             line,
                             "^waitdatabasestart *= *(.*) *$",
                             &start,
                             &length)) {
                strncpy(tempStr, &line[start], minimum(256, length+1));
                *waitDatabaseStart = atoi(tempStr);
                continue;
            }

            /* No acceptable keyword was given in a non-comment line */
            sprintf(msg,
                    "Cannot understand line in '%s': %s",
                    iniFileName,
                    line);
            writeLog('E', msg);
            err = E_ERROR;
        }
        fclose(fIni);
        free(r);
        if (err == E_ERROR) {
            /* Check for the last line */
            exitInit = 1;
        }
        if (exitInit) {
            writeLog('F', "Errors in the initialization file");
            return E_FATAL;
        }

        /* Open the log file in append mode */
        if (createLog(LogFileName) == -1) {
            return E_FATAL;
        }

        if (*TIRDBConnectString == '\0') {
            writeLog('W',
                     "No TIRDB connect string defined in the "
                     "ini file");
        } else {
            if (*resultFileName != '\0') {
                writeLog('W',
                         "Both TIRDB and resultFileName defined "
                         "in the initialization file");
            }
        }

        /* Check that Remote Nodes file exists (if it was defined
           in tatp.ini) and that it has the correct file tag (starting
           the file) */
        if (*RemNodsFileName != '\0') {
            if (openFile(&fTest, RemNodsFileName) != 0) {
                sprintf(msg,
                        "Fatal error: Cannot open file '%s'",
                        RemNodsFileName);
                message('F', msg);
                exit(E_FATAL);
            } else {
                if (readFileLine(fTest, line, 256) == -1) {
                    sprintf(msg, "File '%s' is empty", RemNodsFileName);
                    message('F', msg);
                    message('F',
                            "The file has to start with the "
                            "line //tatp_remotenodes");
                    return E_FATAL;
                }
                /* check the file type from the first line of the file */
                if (!(strncmp(line, "//tatp_remotenodes", 18) == 0)) {
                    sprintf(msg,
                            "File '%s' is not a Remote Nodes file",
                            RemNodsFileName);
                    message('F', msg);
                    message('F', "Check first line (//tatp_remotenodes)");
                    return E_FATAL;
                }
                fclose(fTest);
                sprintf(msg, "Using Remote Nodes file '%s'.", RemNodsFileName);
                message('D', msg);
            }
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      initComm
 *
 * Initializes the communication (socket) system.
 *
 * Parameters:
 *      none
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int initComm( )
{
        int err;

        /* Initialize the communication system */
        err = initializeCommunicationGlobal();
        if (err) {
            writeLog('F', "Cannot initialize the communication system");
            return E_FATAL;
        }
        err = initializeCommunication(&g_comm, "CONTROL");
        if (err) {
            writeLog('F', "Cannot initialize the communication system");
            return E_FATAL;
        }
        /* Create the socket listener for Main or Remote Control */
        err = createListener(&g_comm, controlModulePortNumber);
        if (err) {
            writeLog('F', "Cannot create the socket listener");
            return E_FATAL;
        }
        /* Initialize the messaging system */
        err = initializeMessaging(&g_comm);
        if (err) {
            writeLog('F', "Cannot initialize the messaging system");
            return E_FATAL;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      readRemNodsFile
 *
 * Read the Remote Nodes file and fills up the 'remControls'
 * structure accordingly.
 *
 * Parameters:
 *      RemNodsFileName
 *          remote nodes file name
 *
 * Return value:
 *      0  - success
 *      E_ERROR  - error
 */
int readRemNodsFile(char *RemNodsFileName)
{
        FILE *fRN = NULL;
        /* The first line indicator */
        int firstline;
        int err;
        int remContrInd;
        int remControlId;
        char line[256];
        char msg[300];

        char *strptr = NULL;

        /* the regular expression strucure */
        regex_t *r;
        /* used with regex */
        int count, starts[100], lengths[100];

        /* Open and handle the fiel */
        if (openFile(&fRN, RemNodsFileName) != 0) {
            /* This should not actually happen, because the file was checked
               to exist in parseOptions */
            sprintf(msg, "Cannot open Remote Nodes file '%s'",
                    RemNodsFileName);
            message('F', msg);
            return E_FATAL; /* Fatal error */
        }
        sprintf(msg, "Remote Nodes File '%s'", RemNodsFileName);
        message('I', msg);

        firstline = 1;
        err = 0;
        r = (regex_t *) malloc(sizeof(regex_t));
        if (r == NULL) {
            writeLog('E', "Cannot reserve memory for regex_t");
            return E_ERROR;
        }
        /* The information is stored to 'remControls' starting from the
           index 1 (and not 0) */
        remContrInd = 1;
        /* Remote control IDs start from -3 and go downwards
           (Main control id is -2) */
        remControlId = REMOTE_CONTROL_ID_BASE;
        /* Iterate through the Remote Nodes file */
        while (readFileLine(fRN, line, 256) != -1) {
            /* First remove potential comments from the line just read */
            removeComment(line);
            if (*line == '\0') {
                /* the line is empty */
                continue;
            }
            message('D', line);

            /* Extract the information about the remote nodes from
               the file lines */
            if (!multiMatch(r, line, "^ *(.*) *= *\"(.*)\" +\"(.*)\" *$",
                            &count, starts, lengths)) {
                if (count != 4) {
                    sprintf(msg, "Wrong number of parameters for a Remote "
                            "Node (%s)", line);
                    message('F', msg);
                    return E_FATAL; /* Fatal error */
                }
                strncpy(remControls[remContrInd].name, &line[starts[1]],
                        minimum(lengths[1], W_L));
                remControls[remContrInd].name[minimum(lengths[1], W_L)] = '\0';
                strncpy(remControls[remContrInd].ip, &line[starts[2]],
                        minimum(lengths[2], W));
                remControls[remContrInd].ip[minimum(lengths[2], W)] = '\0';

                remControls[remContrInd].port = CONTROL_PORT;
                strptr = strstr (remControls[remContrInd].ip, ":");
                if (strptr != NULL) {
                    /* specified port */
                    remControls[remContrInd].port = atoi(strptr+1);
                    *strptr = '\0';
                }
                strncpy(remControls[remContrInd].targetDBdsn, &line[starts[3]],
                        minimum(lengths[3], W_L));
                remControls[remContrInd].
                    targetDBdsn[minimum(lengths[3], W_L)] = '\0';

                if ((trim(remControls[remContrInd].name) != 0) ||
                    (trim(remControls[remContrInd].ip) != 0) ||
                    (trim(remControls[remContrInd].targetDBdsn) != 0)) {
					sprintf(msg, "Remote node file handling error "
                            "(memory allocation error)");
					message('F', msg);
					return E_FATAL; /* Fatal error */
                }

                /* Check that the information was actually given */
                if (strlen(remControls[remContrInd].name) == 0) {
                    sprintf(msg, "No name given for a Remote Node");
                    message('F', msg);
                    return E_FATAL; /* Fatal error */
                }
                if (strlen(remControls[remContrInd].ip) == 0) {
                    sprintf(msg, "No IP given for a Remote Node %s",
                            remControls[remContrInd].name);
                    message('F', msg);
                    return E_FATAL; /* Fatal error */
                }
                if (strlen(remControls[remContrInd].targetDBdsn) == 0) {
                    sprintf(msg, "No target DB DSN given for a Remote Node %s",
                            remControls[remContrInd].name);
                    message('F', msg);
                    return E_FATAL; /* Fatal error */
                }

                remControls[remContrInd].defined = 1;
                remControls[remContrInd].remoteControlId = remControlId;

                remContrInd++;
                remControlId--;
                if (remContrInd > (MAX_CONNECTIONS)) {
                    sprintf(msg, "Too many Remote Nodes (>%d) defined in (%s)",
                            MAX_CONNECTIONS-1, RemNodsFileName);
                    message('F', msg);
                    return E_FATAL; /* Fatal error */
                }
            }
        } /* Next line */

        /* regex is not needed any more */
        free(r);
        fclose(fRN);
        return 0;
}

/*##**********************************************************************\
 *
 *      readDDF
 *
 * Reads the DDF file and fills up the ddf data structure.  Reads the
 * database configuration file (defined in DDF) and stores it to
 * TIRDB.
 *
 * Parameters:
 *      ddffilename
 *		    DDF file name
 *
 *      ddf
 *          pointer to ddf struct
 *
 * Return value:
 *      0  - success
 *      E_ERROR  - error
 */
int readDDF(char *ddffilename, struct ddfs *ddf)
{
        FILE *fDDF = NULL;
        FILE *fTest = NULL;
        /* The first line indicator */
        int firstline;
        int err;
        char line[256];
        char msg[300];
        /* the regular expression strucure */
        regex_t *r;
        /* start and length (substring) used with simple regex */
        int start, length;

        /* Open and handle the DDF */
        if (openFile(&fDDF, ddffilename) != 0) {
            /* This should not actually happen, because the file was checked
               to exist in parseOptions */
            sprintf(msg, "Cannot open DDF %s", ddffilename);
            message('F', msg);
            return E_FATAL; /* Fatal error */
        }
        sprintf(msg, "Data Definition File '%s'", ddffilename);
        message('I', msg);

        firstline = 1;
        err = 0;
        r = (regex_t *) malloc(sizeof(regex_t));
        if (r == NULL) {
            writeLog('E', "Cannot reserve memory for regex_t");
            return E_ERROR;
        }
        /* Iterate through the DDF file */
        while (readFileLine(fDDF, line, 256) != -1) {
            /* First remove potential comments from the line just read */
            removeComment(line);

            if (*line == '\0') {
                /* the line is empty */
                continue;
            }
            message('D', line);

            /* Try to find the acceptable keywords from the line.
               If found, copy the keyword value to a variable */
            if (!simpleMatch(r, line, "^db_name *= *\"(.*)\" *$",
                             &start, &length)) {
				strncpy(ddf->db_name, &line[start], minimum(W, length));
				ddf->db_name[length] = '\0';
				continue;
            }
            if (!simpleMatch(r, line, "^db_connect *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->db_connect, &line[start], minimum(W_L, length));
                ddf->db_connect[length] = '\0';
                continue;
            }
            if (!simpleMatch(r, line, "^db_version *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->db_version, &line[start], minimum(W, length));
                ddf->db_version[length] = '\0';
                continue;            
            }
            if (!simpleMatch(r, line, "^os_name *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->os_name, &line[start], minimum(W, length));
                ddf->os_name[length] = '\0';
                continue;
            }
            if (!simpleMatch(r, line, "^os_version *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->os_version, &line[start], minimum(W, length));
                ddf->os_version[length] = '\0';
                continue;
            }
            if (!simpleMatch(r, line, "^hardware_id *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->hardware_id, &line[start], minimum(W, length));
                ddf->hardware_id[length] = '\0';
                continue;
            }
            if (!simpleMatch(r, line, "^configuration_code *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->configuration_code, &line[start],
                        minimum(W, length));
                ddf->configuration_code[length] = '\0';
                continue;
            }
            if (!simpleMatch(r, line, "^configuration_file *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->configuration_file_name, &line[start],
                        minimum(W_L, length));
                ddf->configuration_file_name[length] = '\0';
                continue;
            }
            if (!simpleMatch(r, line, "^configuration_comments *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->configuration_comments, &line[start],
                        minimum(W_EL, length));
                ddf->configuration_comments[length] = '\0';
                continue;
            }
            if (!simpleMatch(r, line, "^targetdbinit *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->db_initfile, &line[start],
                        minimum(FILENAME_LENGTH, length));
                ddf->db_initfile[length] = '\0';
                continue;
            }
            if (!simpleMatch(r, line, "^connectioninit *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->db_connect_initfile, &line[start],
                        minimum(FILENAME_LENGTH, length));
                ddf->db_connect_initfile[length] = '\0';
                continue;
            }
            if (!simpleMatch(r, line, "^targetdbschema *= *\"(.*)\" *$",
                             &start, &length)) {
		        strncpy(ddf->db_schemafile, &line[start],
                        minimum(FILENAME_LENGTH, length));
                ddf->db_schemafile[length] = '\0';
                continue;
            }
            if (!simpleMatch(r, line, "^transaction_file *= *\"(.*)\" *$",
                             &start, &length)) {
		        strncpy(ddf->db_transactionfile, &line[start],
                        minimum(FILENAME_LENGTH, length));
                ddf->db_transactionfile[length] = '\0';
                continue;
            }
            if (*ddf->db_transactionfile == '\0') {
	          	sprintf(msg, "DDF parameter: no 'transaction_file' defined");
                message('F', msg);
                return E_FATAL;
            }

            if (!simpleMatch(r, line, "^targetdbschemaname *= *\"(.*)\" *$",
                             &start, &length)) {
                if (length == 0) {
                    sprintf(msg,
                            "Fatal error: empty DBSchemaName");
                    message('F', msg);
                    free(r);
                    fclose(fDDF);
                    return E_FATAL;
                }
                strncpy(ddf->db_schemaname, &line[start],
                        minimum(W-1, length));
                strcat(ddf->db_schemaname, ".");
                length++;
                ddf->db_schemaname[length] = '\0';
                continue;
            }

            sprintf(msg, "Unknown keyword in DDF: %s", line);
            message('F', msg);
            free(r);
            fclose(fDDF);
            return E_FATAL; /* Fatal error */
        } /* Next line */

        /* Try to determine target DBMS server version automatically
           if db_version is not given in DDF */
        if (ddf->db_version[0] == '\0') {
            if (ddf->db_connect[0] == '\0') {
                sprintf(msg, "'db_connect' must be defined in DDF file "
                        "in order to fetch db_version directly from the target database.");
                message('F', msg);
                free(r);
                fclose(fDDF);
                return E_FATAL;
            }
            if (getTargetDBVersion(&server, ddf->db_connect, ddf->db_version)) {
                sprintf(msg, "Unable to determine database version "
                        "automatically. Please set 'db_version' manually and "
                        "re-run.");
                free(r);
                fclose(fDDF);
                message('F', msg);
                return E_FATAL ;
            }
        }
        
        /* regex is not needed any more */
        free(r);
        fclose(fDDF);

        /* Check that TargetDBInit file exists
           and that it has the correct file tag (starting the file) */
        if (*(ddf->db_initfile) != '\0') {
            if (openFile(&fTest, ddf->db_initfile) != 0) {
                sprintf(msg,
                        "Fatal error: Cannot open DB init file '%s'",
                        ddf->db_initfile);
                message('F', msg);
                return E_FATAL;
            } else {
                if (readFileLine(fTest, line, 256) == -1) {
                    sprintf(msg, "File '%s' is empty", ddf->db_initfile);
                    message('F', msg);
                    message('F',
                            "The file has to start with the line //tatp_sql");
                    return E_FATAL;
                }
                /* check the file type from the first line of the file */
                if ((!(strncmp(line, "//tatp_sql", 10) == 0))
                    && (!(strncmp(line, "--tatp_sql", 10) == 0))) {
                    sprintf(msg,
                            "File '%s' is not a DB initialization file",
                            ddf->db_initfile);
                    message('F', msg);
                    message('F', "Check first line (//tatp_sql)");
                    return E_FATAL;
                }
                fclose(fTest);
                sprintf(msg,
                        "Using DB initialization file '%s'.",
                        ddf->db_initfile);
                message('D', msg);
            }
        }

        /* Check that TargetSchemaInit file exists (if it was defined
           in tatp.ini) and that it has the correct file tag (starting
           the file) */
        if (*(ddf->db_schemafile) != '\0') {
            if (openFile(&fTest, ddf->db_schemafile) != 0) {
                sprintf(msg,
                        "Fatal error: Cannot open DB schema file '%s'",
                        ddf->db_schemafile);
                message('F', msg);
                return E_FATAL;
            } else {
                if (readFileLine(fTest, line, 256) == -1) {
                    sprintf(msg, "File '%s' is empty", ddf->db_schemafile);
                    message('F', msg);
                    message('F', "The file has to start with "
                            "the line '//tatp_sql'");
                    return E_FATAL;
                }
                /* check the file type from the first line of the file */
                if ((!(strncmp(line, "//tatp_sql", 10) == 0))
                    && (!(strncmp(line, "--tatp_sql", 10) == 0))) {
                    sprintf(msg,
                            "File '%s' is not a DB schema file",
                            ddf->db_schemafile);
                    message('F', msg);
                    message('F', "Check first line (//tatp_sql)");
                    return E_FATAL;
                }
                fclose(fTest);
                sprintf(msg, "Using DB schema file '%s'.", ddf->db_schemafile);
                message('D', msg);
            }
        }

        if (*ddf->db_connect == '\0') {
            message('F', "Missing 'db_connect' in DDF");
            return E_FATAL; /* Fatal error */
        }

        return 0;
}

/*##**********************************************************************\
 *
 *      checkDDFparameters
 *
 * Checks if the parameters for TIRDB access are present.*
 *
 * Parameters:
 *      ddf
 *          pointer to ddf struct
 *
 * Return value:
 *      0  - success
 *      E_ERROR  - error
 */
int checkDDFparameters(const struct ddfs *ddf)
{
        /* Check the data */
        if (*ddf->db_name == '\0') {
            message('F', "Missing 'db_name' in DDF");
            return E_FATAL; 
        } else {
            if (strlen(ddf->db_name) > 32) {
                message('F', "Maximum length for 'db_name' in DDF is 32 characters");
                return E_FATAL;
            }
        }
        if (*ddf->db_version == '\0') {
            message('F', "Missing 'db_version' in DDF");
            return E_FATAL; 
        } else {
            if (strlen(ddf->db_version) > 32) {
                message('F', "Maximum length for 'db_version' in DDF is 32 characters");
                return E_FATAL; 
            }
        }
        if (*ddf->os_name == '\0') {
            message('F', "Missing 'os_name' in DDF");
            return E_FATAL; 
        } else {
            if (strlen(ddf->os_name) > 32) {
                message('F', "Maximum length for 'os_name' in DDF is 32 characters");
                return E_FATAL; 
            }
        }
        if (*ddf->os_version == '\0') {
            message('F', "Missing 'os_version' in DDF");
            return E_FATAL; 
        } else {
            if (strlen(ddf->os_version) > 32) {
                message('F', "Maximum length for 'os_version' in DDF is 32 characters");
                return E_FATAL; 
            }
        }
        if (*ddf->hardware_id == '\0') {
            message('F', "Missing 'hardware_id' in DDF");
            return E_FATAL; 
        } else {
            if (strlen(ddf->hardware_id) > 32) {
                message('F', "Maximum length for 'hardware_id' in DDF is 32 characters");
                return E_FATAL; 
            }
        }
        if (*ddf->configuration_file_name == '\0') {
            message('F', "Missing 'configuration_file' in DDF");
            return E_FATAL; 
        }
        if (*ddf->configuration_code == '\0') {
            message('F', "Missing 'configuration_code' in DDF");
            return E_FATAL; 
        }
        /* Configuration_comments is not mandatory so its
           not checked */

        return 0;
}

/*##**********************************************************************\
 *
 *      spawnError
 *
 * Writes out possible error message returned by 'errno'
 * (used while trying to spawn processes)
 *
 * Parameters:
 *      none
 * Return value:
 *      none
 */
void spawnError( )
{
        switch (errno) {
            case E2BIG:
                writeLog('E', "Argument list exceeds 1024 bytes");
                break;
            case EINVAL:
                writeLog('E', "Mode argument is invalid");
                break;
            case ENOENT:
                writeLog('E', "File or path is not found");
                break;
            case ENOEXEC:
                writeLog('E',
                         "Specified file is not executable or has invalid "
                         "executable-file format");
                break;
            case ENOMEM:
                writeLog('E', "Not enough memory is available to execute "
                         "new process");
                break;
            default:
                break;
        }
}

/*##**********************************************************************\
 *
 *      spawnStatistics
 *
 * Launch Statistics as a separate asynchronous process.
 *
 * Parameters:
 *      tdf
 *          Pointer to the test definition structure,
 *          which holds the information related to the
 *			current session.
 *
 *      bmrs
 *          Pointer to the benchmark structure,
 *          which holds the information related to benchmarks.
 *
 *      TIRDBConnectString
 *          Connect string of the Test Input and
 *			Result Database
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int spawnStatistics(struct tdfs *tdf, const struct bmr *bmrs,
					int storeResults, char *TIRDBConnectString,
                    char *resultFileName)
{
        int err;
        int j;
        char rampup[W];
        char throughput_resolution[W];
        char verbose[W];
        char testRunId[W];
        char names[W_EL];        /* Parameters containing transaction names */
#ifndef WIN32
        char **statistics_argv;
        char *charptr;
        int tr_amount = 0;
#endif
        char strExecutable[W_L] = {'\0'};

        strncpy(strExecutable, strProgramDir, W_L);
        strncat(strExecutable, STATISTICS_BINARY_NAME,
                W_L-strlen(strExecutable));

        /* Convert number parameters to character strings */
        itoa(bmrs->warm_up_duration, rampup, 10);
        itoa(g_log.verbose, verbose, 10);
        itoa(tdf->throughput_resolution, throughput_resolution, 10);
        itoa(bmrs->test_run_id, testRunId, 10);

        *names = 0;

        if (bmrs->cmd_type == RUN || bmrs->cmd_type == RUN_DEDICATED) {
            /* Compose the string of transactions (names) */
            for (j = 0; tdf->tr_mixes[bmrs->
                                      transaction_mix_ind].tr_props[j].
                     transact[0] != '\0'; j++) {
                if (W_EL - strlen(names) <
                    strlen(tdf->tr_mixes[bmrs->
                                         transaction_mix_ind].tr_props[j].
                           transact) + 2) {
                    /* We are running out of space for the names */
                    writeLog('E', "Cannot spawn Statistics: too long "
                             "command line");
                    return E_ERROR;
                }
                strcat(names, " ");
                strncat(names, tdf->tr_mixes[bmrs->
                                             transaction_mix_ind].tr_props[j].
                                             transact, W_L);
#ifndef WIN32
                tr_amount++;
#endif
            }
        }

#ifdef WIN32
        /* Start Statistics */
        statistics_pid = _spawnl(_P_NOWAIT, strExecutable,
                                 STATISTICS_BINARY_NAME,
                                 testRunId,
                                 rampup,
                                 tdf->control_host,
                                 (storeResults == MODE_TO_TIRDB && (*TIRDBConnectString != 0) )
                                   ? TIRDBConnectString : "NULL",
                                 *resultFileName != 0 ? resultFileName : "NULL",
                                 throughput_resolution,
                                 verbose,
                                 names,
                                 NULL);
        err = (int)statistics_pid;
#else
        /* 'names' contains names of the transactions, separated by
           spaces (" ") tr_amount contains amount of transactions in
           the transaction mix  */
        statistics_argv =
            (char**)malloc((STATISTICS_STATIC_ARGC + tr_amount + 1)*sizeof(char*));
        for (j = 0; j < (STATISTICS_STATIC_ARGC + tr_amount); j++) {
            statistics_argv[j] = (char*)malloc(W_L*sizeof(char));
        }
        strncpy(statistics_argv[0], STATISTICS_BINARY_NAME, 11);
        strncpy(statistics_argv[1],testRunId, W);
        strncpy(statistics_argv[2],rampup, W);
        strncpy(statistics_argv[3],tdf->control_host, W_L);
        if ( (storeResults == MODE_TO_TIRDB) && (*TIRDBConnectString != 0) ) {
            strncpy(statistics_argv[4],TIRDBConnectString, W_L);
        } else {
            strncpy(statistics_argv[4],"NULL", 5);
        }
        if (*resultFileName != 0) {
            strncpy(statistics_argv[5],resultFileName, W_L);
        } else {
            strncpy(statistics_argv[5],"NULL", 5);
        }

        strncpy(statistics_argv[6], throughput_resolution, W);
        strncpy(statistics_argv[7], verbose, W);
        j = STATISTICS_STATIC_ARGC;  /* current index in parameter array */
        charptr = strtok(names," ");
        while (charptr != NULL) {
            strncpy(statistics_argv[j], charptr, W_L);
            charptr = strtok(NULL, " ");
            j++;
        }
        statistics_argv[j] = NULL;
        err = 0;
        /* Start Statistics */
#if !defined __hpux
        err = posix_spawn (&statistics_pid, strExecutable, NULL, NULL,
                           statistics_argv, environ);
#else
        statistics_pid = fork();
        if (statistics_pid<0) {
            /* Error */
            perror("Creating a statistics process failed");
            err = statistics_pid;
        } else if (statistics_pid==0) {
            /* Child */
            err = execv(strExecutable, statistics_argv);
        }
        sleep(1); /* Ensure that the child has been launched */
#endif

        /* free allocated memory */
        for (j = 0; j < (STATISTICS_STATIC_ARGC + tr_amount); j++) {
            free(statistics_argv[j]);
        }
        free(statistics_argv);
#endif /* WIN32 */

        if (err == -1) {
            writeLog('E', "Cannot execute Statistics:");
            spawnError();
            return E_ERROR;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      spawnClientsInNetwork
 *
 * Reads the tdf structure and sends all the required test parameters
 * to all the remote controls, including the transaction file
 * content. Also launches own local clients (in the main control
 * machine, that is) for execution.
 *
 * Parameters:
 *      ddf
 *          Pointer to the data definition structure
 *
 *      tdf
 *          Pointer to the test definition structure
 *
 *      bmrs
 *          Pointer to the benchmark structure ,
 *          which holds the information related to benchmarks.
 *
 *      workDirBase
 *
 *      mainClientProcesses
 *          returns amount of clients in the Main Node
 *
 *      DBSchemaName
 *          database schema name
 *
 *      waitDatabaseStart
 *          value containing wait method for database start
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int spawnClientsInNetwork(struct ddfs *ddf, struct tdfs *tdf, const struct bmr *bmrs,
                          char *workDirBase, int *mainClientProcesses,
                          char *DBSchemaName, int waitDatabaseStart) {
        
        /* Counter and count of Clients to spawn */
        int totalRemoteClients;
        int j, k, stat, clientStart;
        char prob[W];             /* Buffer for ascii number */
        char *namesAndProbs;
        int tr_amount = 0;
        struct message_dataS data;
        char txt_buf[256];
        int remControlIndex;
        struct clientStartParameters csp;
        int mainClients;

        /* these are mandatory parameters */
        strncpy(csp.db_connect, ddf->db_connect, W_L);
        strncpy(csp.transaction_file, ddf->db_transactionfile, W_L);

        strncpy(csp.statistics_host, tdf->statistics_host, W);

        /* support only client directory for now */
        /* TODO: workDir support */
        strcpy(csp.workDir, ".");
        csp.workDir[1] = '\0';

        if (*DBSchemaName != '\0') {
            strncpy(csp.db_schemaname, DBSchemaName, W);
        } else {
            /* must not be empty */
            strcpy(csp.db_schemaname, ".");
            csp.db_schemaname[1] = '\0';
        }
        
        /* optional */
        if (*(ddf->db_connect_initfile) != '\0') {
            strncpy(csp.connection_init_file, ddf->db_connect_initfile, W_L);
        } else {
            /* must not be empty anyway */
            strcpy(csp.connection_init_file, ".");
            csp.connection_init_file[1] = '\0';
        }

        csp.rampup = bmrs->warm_up_duration;
        csp.rampupPlusLimit = bmrs->warm_up_duration + bmrs->run_duration;
        csp.verbose = g_log.verbose;
        csp.population_size = bmrs->subscribers;
        csp.uniform = tdf->uniform;
        csp.testRunId = bmrs->test_run_id;
        csp.namesAndProbs[0] = '\0';
        csp.check_targetdb = tdf->check_targetdb;
        csp.serial_keys = bmrs->serial_keys;
        csp.commitblock_size = bmrs->commitblock_size;
        strncpy(csp.db_schemafilename, ddf->db_schemafile, FILENAME_LENGTH);
        csp.reportTPS = reportTPS;
        csp.detailedStatistics = showDetailedStatistics;
        csp.waitDatabaseStart = waitDatabaseStart;

        csp.operation_mode = bmrs->cmd_type;        
        namesAndProbs = csp.namesAndProbs;

        if (csp.operation_mode == RUN_DEDICATED) {
            *mainClientProcesses = 1;
        } else {
            /* Loop through the remote definitions and count the total number of
               remote clients to be used in the test */
            totalRemoteClients = 0;
            for (j = 0; tdf->client_distributions[bmrs->
                                                  client_distribution_ind].
                     rem_loads[j].remControls_index;
                 j++) {
                totalRemoteClients +=
                    tdf->client_distributions[bmrs->client_distribution_ind].
                    rem_loads[j].remLoad;
            }
            mainClients = tdf->client_distributions[bmrs->
                                                    client_distribution_ind].
                                                    localLoad;
            if (csp.operation_mode == POPULATE
                || csp.operation_mode == POPULATE_CONDITIONALLY
                || csp.operation_mode == POPULATE_INCREMENTALLY) {
                if (totalRemoteClients > 0) {
                    writeLog('E', "Cannot spawn Clients: remote clients must not be "
                             "defined for 'populate' command");
                    return E_ERROR;
                }
                *mainClientProcesses = 1;
            } else {
                *mainClientProcesses = tdf->client_distributions[bmrs->
                                                                 client_distribution_ind].
                                                                 localLoadProcesses;
            }
            clientStart = mainClients+1;
        }

        if (csp.operation_mode == RUN || csp.operation_mode == RUN_DEDICATED) {
            /* Compose the character string of benchmark names and probabilities */
            for (j = 0; tdf->tr_mixes[bmrs->transaction_mix_ind].
                     tr_props[j].transact[0] != '\0'; j++) {
                if (W_EL - strlen(namesAndProbs)
                    < strlen(tdf->tr_mixes[bmrs->
                                           transaction_mix_ind].tr_props[j].
                             transact) + 3) {
                    /* We are running out of space for the names and probs. */
                    /* Note: two additional spaces counted above (3) */
                    writeLog('E', "Cannot spawn Clients: too long command line");
                    return E_ERROR;
                }
                strcat(namesAndProbs, " ");
                strncat(namesAndProbs, tdf->tr_mixes[bmrs->
                                                     transaction_mix_ind].
                        tr_props[j].transact, W_L);
                tr_amount++;
                strcat(namesAndProbs, " ");
                itoa(tdf->tr_mixes[bmrs->
                                   transaction_mix_ind].tr_props[j].prob, prob, 10);
                if (W_EL - strlen(namesAndProbs) < strlen(prob) + 1) {
                    /* We are running out of space for the names and probs. */
                    writeLog('E', "Cannot spawn Clients: too long command line");
                    return E_ERROR;
                }
                strncat(namesAndProbs, prob, W);
            }
            if (csp.operation_mode == RUN_DEDICATED) {
                mainClients = tr_amount;
                tdf->client_distributions[bmrs->
                                          client_distribution_ind].localLoad = mainClients;
                /* do not run remote clients */
            } else {
                stat = 0;
                /* Handle all the remote control related
                   if one or more remotes exists */
                if (tdf->client_distributions[bmrs->client_distribution_ind].
                    rem_loads[0].remControls_index) {

                    /* Send the transaction file to each remote control */
                    for (j = 0; tdf->client_distributions[bmrs->
                                                          client_distribution_ind].
                             rem_loads[j].
                             remControls_index; j++) {
                        remControlIndex =
                            tdf->client_distributions[bmrs->client_distribution_ind].
                            rem_loads[j].remControls_index;
                        stat = sendFileToSocket(remControls[remControlIndex].sck,
                                                MAIN_CONTROL_ID,
                                                ddf->db_transactionfile,
                                                TRANSACTIONFILE);
                        if (stat != 0) {
                            sprintf(txt_buf, "Error sending a file to the remote "
                                    "control %d",
                                    remControls[remControlIndex].remoteControlId);
                            message('E', txt_buf);
                            break;
                        }
                    }
                    message('D', "Transaction file sent to remote controls");

                    /* Send all the test parameters to each remote control */

                    if (stat == 0) {
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            /* Send the number of client threads to be run in the remote */
                            itoa(tdf->client_distributions[
                                         bmrs->client_distribution_ind].
                                 rem_loads[j].remLoad,
                                 data.sdata.testparam.data, 10);
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;

                            /* Send min and max subscriber id's */
                            itoa(tdf->client_distributions[
                                         bmrs->client_distribution_ind].
                                 rem_loads[j].min_subs_id,
                                 data.sdata.testparam.data, 10);
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                            itoa(tdf->client_distributions[
                                         bmrs->client_distribution_ind].
                                 rem_loads[j].max_subs_id,
                                 data.sdata.testparam.data, 10);
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                            itoa(clientStart, data.sdata.testparam.data, 10);

                            /* Send the ordinal number of the first client to be run
                               in the remote */
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                            itoa(tdf->client_distributions[
                                         bmrs->client_distribution_ind].
                                 rem_loads[j].remLoadProcesses,
                                 data.sdata.testparam.data, 10);
                            /* Send the process amount to be run in the remote */
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                            clientStart = clientStart
                                + tdf->client_distributions[bmrs->
                                                            client_distribution_ind].
                                rem_loads[j].remLoad;
                        }
                    }
                    if (stat == 0) {
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            /* Send target DSN */
                            strncpy(data.sdata.testparam.data,
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].
                                    targetDBdsn,
                                    128);
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }

                    if (stat == 0) {
                        /* send schema name */
                        strncpy(data.sdata.testparam.data, csp.db_schemaname, W);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }

                    if (stat == 0) {
                        /* send schema file name */
                        strncpy(data.sdata.testparam.data, csp.db_schemafilename, FILENAME_LENGTH);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }

                    if (stat == 0) {
                        /* send workDir */
                        strncpy(data.sdata.testparam.data, csp.workDir, W_L);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }                    

                    if (stat == 0) {
                        /* Send the connection init sql file name */
                        strncpy(data.sdata.testparam.data, csp.connection_init_file, W_L);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }

                    if (stat == 0) {
                        /* send population size */
                        itoa(csp.population_size, data.sdata.testparam.data, 10);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index; j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* send uniform parameter */
                        itoa(csp.uniform, data.sdata.testparam.data, 10);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* send rampup time */
                        itoa(csp.rampup, data.sdata.testparam.data, 10);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* send check_targetdb */
                        itoa(csp.check_targetdb, data.sdata.testparam.data, 10);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* send serial_keys */
                        itoa(csp.serial_keys, data.sdata.testparam.data, 10);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* send check_targetdb */
                        itoa(csp.commitblock_size, data.sdata.testparam.data, 10);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }               
                    if (stat == 0) {
                        /* send test time */
                        itoa(csp.rampupPlusLimit, data.sdata.testparam.data, 10);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* send statistics host name*/
                        strncpy(data.sdata.testparam.data, tdf->statistics_host, 128);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* send test run id */
                        itoa(csp.testRunId, data.sdata.testparam.data, 10);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* send amount of transactions */
                        itoa(tr_amount, data.sdata.testparam.data, 10);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* send name of the transaction file */
                        strncpy(data.sdata.testparam.data,
                                ddf->db_transactionfile, 128);
                        for (j = 0;
                             tdf->client_distributions[
                                     bmrs->client_distribution_ind].rem_loads[j].
                                 remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* send verbosity level */
                        itoa(csp.verbose, data.sdata.testparam.data, 10);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->
                                                        client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* send throughput resolution */
                        itoa(tdf->throughput_resolution, data.sdata.testparam.data, 10);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* send TPS flag */
                        itoa(csp.reportTPS, data.sdata.testparam.data, 10);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* send DetailedStats flag */
                        itoa(csp.detailedStatistics, data.sdata.testparam.data, 10);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* send WaitDatabaseStart value */
                        itoa(csp.waitDatabaseStart, data.sdata.testparam.data, 10);
                        for (j = 0;
                             tdf->client_distributions[bmrs->client_distribution_ind].
                                 rem_loads[j].remControls_index;
                             j++) {
                            data.utime = time(NULL);
                            stat = sendDataS(
                                    remControls[tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[j].remControls_index].sck,
                                    MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                            if (stat != 0) break;
                        }
                    }
                    if (stat == 0) {
                        /* Loop through the transaction names and probabilities and
                           send them to the remote controls */
                        for (j = 0;
                             tdf->tr_mixes[bmrs->transaction_mix_ind].
                                 tr_props[j].transact[0] != '\0'; j++) {
                            strncpy(data.sdata.testparam.data,
                                    tdf->tr_mixes[bmrs->transaction_mix_ind].
                                    tr_props[j].transact, W_L);
                            strcat(data.sdata.testparam.data, " ");
                            itoa(tdf->tr_mixes[bmrs->transaction_mix_ind].
                                 tr_props[j].prob, prob, 10);
                            strncat(data.sdata.testparam.data, prob, W);

                            for (k = 0;
                                 tdf->client_distributions[
                                         bmrs->
                                         client_distribution_ind].rem_loads[k].
                                     remControls_index; k++) {
                                data.utime = time(NULL);
                                stat = sendDataS(
                                        remControls[
                                                tdf->client_distributions[
                                                        bmrs->client_distribution_ind].
                                                rem_loads[k].
                                                remControls_index].sck,
                                        MAIN_CONTROL_ID, MSG_TESTPARAM, &data);
                                if (stat != 0) break;
                            }
                        }
                    }
                    if (stat != 0) {
                        writeLog('E', "Error sending test parameters to "
                                 "remote controls ");
                        return E_ERROR;
                    }
                    else {
                        writeLog('D', "Test parameters sent to the remotes.");
                    }
                    if (sendSpawnClientMessages(tdf, bmrs)) {
                        writeLog('E', "Error sending spawn client messages "
                                 "to remotes controls");
                        return 1;
                    }
                }
            } /* if (csp.operation_mode == RUN_DEDICATED) ... }  else { */
        }

        if (mainClients == 0) {
            return 0;
        }
        else {
            /* Start the local clients running in the Main Node */
            csp.firstClient = 1;
            csp.numOfClients = mainClients;
            csp.tr_amount = tr_amount;
            csp.throughput_resolution = tdf->throughput_resolution;
            csp.numOfProcesses = *mainClientProcesses;
            
            if (*workDirBase != '\0') {
                /* was explicitly defined in INI file */
                strncpy(csp.workDir, workDirBase, W_L);
                /* TODO: could check that the directories [1-n_processes] specified above do exist */
                /* stop if not */
#ifdef ACCELERATOR
            } else if (csp.numOfProcesses > 1) {
                /* multiaccelerator needs separate working directories*/
                strncpy(csp.workDir, DEFAULT_CLIENTDIR_PREFIX, W_L);
                strncpy(workDirBase, DEFAULT_CLIENTDIR_PREFIX, W_L);
                /* TODO: check that the directories exists */
#endif
            } else {
                /* use current directory as client process workdir */                
                strcpy(csp.workDir, ".");
                csp.workDir[1] = '\0';
            }
            
            /* these are already set above */
            /* csp.operation_mode = bmrs->cmd_type;
               csp.numOfProcesses =
               tdf->client_distributions[
               bmrs->client_distribution_ind].localLoadProcesses;
            */

            if ((bmrs->min_subscriber_id > 1)
                && (bmrs->cmd_type == POPULATE_INCREMENTALLY)){
                csp.min_subs_id = bmrs->min_subscriber_id;
                csp.max_subs_id = bmrs->min_subscriber_id + bmrs->subscribers - 1;
            } else {                
                csp.min_subs_id =
                    tdf->client_distributions[bmrs->client_distribution_ind].
                    min_subs_id;
                csp.max_subs_id =
                    tdf->client_distributions[bmrs->client_distribution_ind].
                    max_subs_id;
            }
            csp.reportTPS = reportTPS;
            csp.detailedStatistics = showDetailedStatistics;
            return spawnClients(&csp);
        }
}

/*##**********************************************************************\
 *
 *      sendSpawnClientMessages
 *
 * Sends the SPAWNCLIENTS messages to all the remotes.
 *
 * Parameters:
 *      tdf
 *          TDF session data structure
 *
 *      bmrs
 *          benchmark run data structure
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int sendSpawnClientMessages(struct tdfs *tdf, const struct bmr *bmrs)
{
        int j;
        char txt_buf[256];
        struct message_dataS data;
        /* Loop through the remote definitions and send the
           spawn clients request to each remote control */
        for (j = 0;
             tdf->client_distributions[bmrs->client_distribution_ind].
                                       rem_loads[j].remControls_index; j++) {
            if (sendDataS(remControls[tdf->client_distributions[
                                      bmrs->client_distribution_ind].
                                      rem_loads[j].remControls_index].sck,
                          MAIN_CONTROL_ID, MSG_SPAWNCLIENTS, &data) != 0) {
                sprintf(txt_buf, "Error sending spawn clients request to "
                        "the remote %d", remControls[
                                tdf->client_distributions[
                                        bmrs->
                                        client_distribution_ind].
                                        rem_loads[j].
                                        remControls_index].
                                        remoteControlId);
                message('E', txt_buf);
            }
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      spawnClients
 *
 * Launch desired number of client instances as separate
 * asynchronous processes/threads.
 *
 * Parameters:
 *      csp
 *          Combined client start parameters
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int spawnClients(struct clientStartParameters* csp)
{
        int rc = 0;
        int err, clientNum, clientsPerProcess;
        char **client_argv;
        int j, i, processNum;
        int remainingClients;

#ifdef WIN32
        intptr_t pid;  /* Process Handle */
#else
        char *charptr;
#endif
        char strExecutable[W_L];

        strncpy(strExecutable, strProgramDir, W_L);
        strncat(strExecutable, CLIENT_BINARY_NAME, W_L-strlen(strExecutable));

        /* 'namesAndProbs' contains names and probabilities
           of the transactions, separated by spaces */
        client_argv = (char**)malloc((CLIENT_STATIC_ARGC+(2*csp->tr_amount)+1)
                                     *sizeof(char*));
        for (j = 0; j < (CLIENT_STATIC_ARGC+(2*csp->tr_amount)); j++) {
            client_argv[j] = (char*)malloc(W_L*sizeof(char));
        }
        /* Fill up the client_argv structure */
        strncpy(client_argv[0],CLIENT_BINARY_NAME, W_L);
        strncpy(client_argv[ARGV_WORKDIR], csp->workDir, W_L);
        strncpy(client_argv[ARGV_TRANSACTION_FILE],csp->transaction_file, W_L);
        strncpy(client_argv[ARGV_CONNECTION_INIT_SQL_FILENAME], csp->connection_init_file, W_L);
        strncpy(client_argv[ARGV_DBSCHEMAFILENAME], csp->db_schemafilename, FILENAME_LENGTH);
        strncpy(client_argv[ARGV_DBSCHEMANAME], csp->db_schemaname, W);

#ifdef WIN32
        strncpy(client_argv[ARGV_TEST_DSN], "\"", W_L);
        strncat(client_argv[ARGV_TEST_DSN], csp->db_connect, W_L);
        strncat(client_argv[ARGV_TEST_DSN], "\"", W_L);
#else
        strncpy(client_argv[ARGV_TEST_DSN],csp->db_connect, W_L);
#endif

        itoa(csp->serial_keys, client_argv[ARGV_SERIAL_KEYS], 10);
        itoa(csp->commitblock_size, client_argv[ARGV_COMMITBLOCK_SIZE], 10);
        itoa(csp->check_targetdb, client_argv[ARGV_CHECK_TARGETDB], 10);

        itoa(csp->rampup, client_argv[ARGV_RAMPUP_TIME], 10);
        itoa(csp->rampupPlusLimit, client_argv[ARGV_TEST_TIME], 10);
        itoa(csp->verbose, client_argv[ARGV_LOG_VERBOSITY], 10);
        itoa(csp->throughput_resolution,
             client_argv[ARGV_THROUGHPUT_RESOLUTION], 10);
        strncpy(client_argv[ARGV_STATISTICS_IP],csp->statistics_host, W_L);

        itoa(csp->testRunId, client_argv[ARGV_TEST_ID], 10);
        itoa(csp->population_size, client_argv[ARGV_POPULATION_SIZE], 10);
        itoa(csp->uniform, client_argv[ARGV_UNIFORM], 10);

        itoa(csp->min_subs_id, client_argv[ARGV_MIN_SUBS_ID], 10);
        itoa(csp->max_subs_id, client_argv[ARGV_MAX_SUBS_ID], 10);
        itoa(csp->operation_mode, client_argv[ARGV_OPERATION_TYPE], 10);

        itoa(csp->reportTPS, client_argv[ARGV_REPORT_TPS], 10);
        itoa(csp->detailedStatistics, client_argv[ARGV_DETAILED_STATISTICS],10);

#ifndef WIN32
        j = CLIENT_STATIC_ARGC;   /* current index in parameter array */
        charptr = strtok(csp->namesAndProbs," ");
        /* Iterate through all the sub-string within namesAndProbs */
        while (charptr != NULL) {
            strncpy(client_argv[j], charptr, W_L);
            charptr = strtok(NULL, " ");
            j++;
        }
        client_argv[j] = NULL;
        /* reserve space for client process ids */
        client_pid = (pid_t*)malloc(csp->numOfProcesses * sizeof(pid_t));
        for (i = 0; i < csp->numOfProcesses; i++) {
            client_pid[i] = 0;
        }
#endif

        clientsPerProcess = (int)(csp->numOfClients / csp->numOfProcesses);
        remainingClients = csp->numOfClients % csp->numOfProcesses;
        clientNum = csp->firstClient;

        for (processNum = 0; processNum < csp->numOfProcesses; processNum++) {
            itoa(clientNum, client_argv[ARGV_CLIENT_ID], 10);
            if (processNum == (csp->numOfProcesses - 1)) {
                /* remaining clients */
                clientsPerProcess += remainingClients;
            }
            itoa(clientsPerProcess, client_argv[ARGV_NUM_CLIENT_THREADS], 10);
            itoa(processNum+1, client_argv[ARGV_CLIENT_PROCESS_ID], 10);

            /* client listener port */
            if (controlModuleMode == MODE_REMOTE_CONTROL_PORT_SPECIFIED) {
                /* Remote Control running at the same node
                   with Main Control */
                /* or at a different node, but the port
                   is changed for some reason */
                itoa(CLIENT_PORT_BASE + clientNum - 1,
                     client_argv[ARGV_CLIENT_TCP_LISTEN_PORT], 10);
            } else {
                /* Main Control clients
                   or Remote Control on a different machine */
                itoa(CLIENT_PORT_BASE + (clientNum - csp->firstClient),
                     client_argv[ARGV_CLIENT_TCP_LISTEN_PORT], 10);
            }

            /* port number of Main or Remote Control */
            itoa(controlModulePortNumber,
                 client_argv[ARGV_CONTROL_TCP_LISTEN_PORT], 10);
#ifdef WIN32
            /* Start a client */
            pid = _spawnl(_P_NOWAIT, strExecutable,
                          CLIENT_BINARY_NAME,
                          client_argv[ARGV_WORKDIR],
                          client_argv[ARGV_TRANSACTION_FILE],
                          client_argv[ARGV_CONNECTION_INIT_SQL_FILENAME],
                          client_argv[ARGV_DBSCHEMAFILENAME],
                          client_argv[ARGV_DBSCHEMANAME],
                          client_argv[ARGV_OPERATION_TYPE],
                          client_argv[ARGV_TEST_DSN],
                          client_argv[ARGV_SERIAL_KEYS],
                          client_argv[ARGV_COMMITBLOCK_SIZE],
                          client_argv[ARGV_CHECK_TARGETDB],
                          client_argv[ARGV_RAMPUP_TIME],
                          client_argv[ARGV_TEST_TIME],
                          client_argv[ARGV_LOG_VERBOSITY],
                          client_argv[ARGV_THROUGHPUT_RESOLUTION],
                          client_argv[ARGV_STATISTICS_IP],
                          client_argv[ARGV_CLIENT_ID],
                          client_argv[ARGV_CLIENT_TCP_LISTEN_PORT],
                          client_argv[ARGV_CONTROL_TCP_LISTEN_PORT],
                          client_argv[ARGV_TEST_ID],
                          client_argv[ARGV_POPULATION_SIZE],
                          client_argv[ARGV_MIN_SUBS_ID],
                          client_argv[ARGV_MAX_SUBS_ID],
                          client_argv[ARGV_UNIFORM],
                          client_argv[ARGV_NUM_CLIENT_THREADS],
                          client_argv[ARGV_CLIENT_PROCESS_ID],
                          client_argv[ARGV_REPORT_TPS],
                          client_argv[ARGV_DETAILED_STATISTICS],
                          csp->namesAndProbs,
                          NULL);
            err = (int) pid;
#elif defined __hpux
            client_pid[processNum]=fork();
            if (client_pid[processNum]<0) {
                /* Error */
                writeLog('E', "Cannot execute Client:");
                spawnError();
                rc = E_ERROR;
                goto fin;
            } else if (client_pid[processNum]==0) {
                /* Child */
                err = execv(strExecutable, client_argv);
            }
            sleep(1);
#else
            /* set client id and port number */
            err = posix_spawn (&(client_pid[processNum]),
                               strExecutable, NULL, NULL, client_argv, environ);
#endif
            if (err == -1) {
                char msg[W_L];
                writeLog('E', "Cannot start a Client: ");
                spawnError();
#ifndef WIN32
                sprintf(msg, "Spawned client pid is: %d",
                        client_pid[processNum]);
                message('D', msg);
#endif
                rc = E_ERROR;
                goto fin;
            }
            clientNum += clientsPerProcess;
        } /* for */

fin:
        /* free allocated memory */
        for (j = 0; j < (CLIENT_STATIC_ARGC+(2*csp->tr_amount)); j++) {
            if (client_argv[j] != NULL)
                free(client_argv[j]);
        }

        free(client_argv);
        /* client_pid array will be free'd later in cleanUpClients */

        return rc;
}

/*##**********************************************************************\
 *
 *      waitStatisticsMessage
 *
 * Waits for the benchmark to complete by waiting messages from
 * Statistics. Control receives two messages from Statistics just
 * before Statistics exits. First is the MSG_COMPLETED message with
 * the avg_MQTH value and the second is the MSG_LOGOUT message with
 * the total number of errors from the Statistics.
 *
 * Parameters:
 *      bmrs
 *		    benchmark data structure
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int waitStatisticsMessage(struct bmr *bmrs)
{
        int retval = 0;
        /* Sender ID of the received message */
        int senderID;
        /* Type of the received message. */
        int messageType;
        /* Struct to hold the received data */
        struct message_dataS data;
        /* Flag for while loop */
        int receiving = 1;
        /* Retry counter for the while loop */
        int retryCounter = WAIT_S_RETRIES;
        char msg[W_L];

        while (receiving && retryCounter > 0) {
            retval = receiveDataS(&g_comm, &senderID, &messageType, &data);
            if (retval == 0) {
                if (senderID != STATISTICS_ID) {
                    sprintf(msg,
                            "Received a message from an "
                            "unexpected sender '%d'",
                            senderID);
                    writeLog('E', msg);
                    retval = E_ERROR;
                    /* Continuing to wait correct sender for some time */
                    retryCounter--;
                }
                else {
                    if (messageType == MSG_COMPLETED) {
                        /* This is what we are waiting for */
                        writeLog('D', "Received MSG_COMPLETED from Statistics");
                        bmrs->avg_mqth = data.sdata.reg.data;
                    }
                    else if (messageType == MSG_LOGOUT) {
                        writeLog('D', "Received MSG_LOGOUT from Statistics.");
                        /* This is the final message we are waiting for */
                        if (data.sdata.reg.data != 0) {
                            sprintf(msg,
                                    "Statistics reported total of "
                                    "%d errors from "
                                    "Clients and Statistics",
                                    data.sdata.reg.data);
                            writeLog('I', msg);
                            /* With this line, errors cause the rest of
                               the TDF to be skipped */
                            retval = E_ERROR;
                        }
                        g_log.errorCount += data.sdata.reg.data;
                        receiving = 0; /* Stop waiting for messages from
                                          Statistics */
                    }
                    else if (messageType == MSG_INTR) {
                        writeLog('E',
                                 "Received MSG_INTR from Statistics. "
                                 "Cannot handle "
                                 "it (not implemented)");
                        /* We could do Interrupt handling here (not
                           needed for now) */
                        receiving = 0; /* Stop waiting for messages from
                                          Statistics */
                        retval = E_ERROR;
                    }
                    else {
                        sprintf(msg,
                                "Received an unexpected message "
                                "'%d' from Statistics",
                                messageType);
                        writeLog('E', msg);
                        receiving = 0; /* Stop waiting for messages from
                                          Statistics */
                        retval = E_ERROR;
                    }
                }
            }
            else {
                sprintf(msg,
                        "Error %d at receiveDataS() while waiting message "
                        "from Statistics",
                        retval);
                writeLog('E', msg);
                /* Continuing to wait message for some time */
                retryCounter--;
            }
        }
        if (retryCounter <= 0) {
            writeLog('I', "Giving up");
        }
        return retval;
}

/*##**********************************************************************\
 *
 *      freeBenchmarks
 *
 * Free the memory reserved for benchmarks.
 *
 * Parameters:
 *      bmrs
 *          Pointer to the benchmark structure,
 *          which holds the information related to benchmarks.
 *
 *      num_of_bmrs
 *          The number of benchmarks
 *
 * Return value:
 *      none
 */
void freeBenchmarks(struct bmr *bmrs[], int num_of_bmrs)
{
        int i;

        for (i = 0; i < num_of_bmrs; i++) {
            if (bmrs[i] != NULL) {
                free(bmrs[i]);
                bmrs[i] = NULL;
            }
        }
}

/*##**********************************************************************\
 *
 *      ctrlTDF
 *
 * Controls the execution of desired benchmarks defined in one TDF
 * file. First the TDF is read and data assigned to the tdf data
 * structure, then the data is verified and finally the benchmarks
 * defined in the TDF are executed.
 *
 * Parameters:
 *      ddf
 *			struct of Data Definition File
 *
 *      workDirBase
 *          client process working directory (base)
 *
 *      TIRDBConnectString
 *          Connect string of the Test Input and Result Database
 *
 *      resultFileName
 *          Name of the results file, or empty string if not defined
 *
 *      tdfname
 *          Name of the TDF file to be handled
 *
 *      clientSynchThreshold
 *			Client synchronization threshold value
 *
 *      waitDatabaseStart
 *          value containing wait method for database start
 *
 *      addMissing
 *          boolean 'Add missing values to TIRDB'
 *
 *      dedicatedThreads
 *          boolean 'Run transactions in separate dedicated threads'
 *
 *      testSequence
 *          if not empty string, the Test Sequence from the command line
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int ctrlTDF(struct ddfs *ddf, char *workDirBase, char *TIRDBConnectString, char *resultFileName,
            char *tdfname, int clientSynchThreshold, int waitDatabaseStart,
            int addMissing, int dedicatedThreads, char *testSequence)
{
        char        msg[300];        /* message buffer for writeLog() */
        int         err;             /* error (return value) */
        int         num_of_bmrs;     /* count of bm's */
        int         i;
        int         senderId;        /* For communicating with Statistics */
        int         messageType;
        int         mainClientProcesses = 0;
        int         result = E_OK;

        struct tdfs tdf;             /* common TDF data */
        struct bmr  *bmrs[MAX_BM];   /* benchmarks in the current TDF */
        /* The test timer that is started just before the synch. check
           of the clients */
        struct timertype_t test_timer;
        struct message_dataS data;
        double timerResolutionMean;
        FILE   *fSQL;
#ifndef WIN32
        pid_t wpid;
        int status;
#endif
#ifdef ACCELERATOR
        char c;
#endif

        int storeResults = MODE_TO_LOGS_ONLY;
        if (*TIRDBConnectString != '\0') {
            storeResults = MODE_TO_TIRDB;
        } else if (*resultFileName != '\0') {
            storeResults = MODE_TO_SQLFILE;
        }

        initTimer(&test_timer, TIMER_MILLISECOND_SCALE);
        message('I', "Estimating system timer resolution...");
        estimateTimerResolution(
                &test_timer,
                &timerResolutionMean);
        sprintf(msg, "System timer resolution, usec: %f",
                timerResolutionMean * 1000000.0);
        message('I', msg);

        num_of_bmrs = 0;

        /* Open and handle the TDF */
        /* populates bmrs structure */
        err = readTDF(tdfname, &tdf, bmrs, &num_of_bmrs, testSequence);

        if (err != 0) {
            /* The error was reported in readTDF */
            goto error;
        }

        /* If TIRDB is in use, check it */
        if (storeResults == MODE_TO_TIRDB) {
            result = checkTIRDB(TIRDBConnectString, ddf, addMissing);
            if (result == E_FATAL) {
                /* could not connect to TIRDB */
                if (*resultFileName != '\0') {
                    writeLog('E', "TIRDB initialization for session failed "
                             "... using result file instead");
                    /* strncpy(resultFileName,
                            DEFAULT_STATISTICS_RESULTFILENAME,
                            W_L);*/
                    *TIRDBConnectString = '\0';
                    storeResults = MODE_TO_SQLFILE;
                } else {
                    err = E_FATAL;
                    goto error;
                }
            } else if (result == E_ERROR) {
                writeLog('E',
                         "Cannot process the TDF. Checking TDF data against "
                         "TIRDB failed");
                /* Not fatal, because we can handle the next TDF */
                err = E_ERROR;
                goto error;
            }
        }

        /* If TIRDB is in use, initialize TIRDB for session (DDF/TDF) data */
        setDateTimeNow(&(tdf.start_date), &(tdf.start_time));

        if (storeResults == MODE_TO_TIRDB) {
            if (initialize_tirdb_for_session(TIRDBConnectString, ddf, &tdf) ) {
                writeLog('E', "TIRDB initialization for session failed");
                /* Not fatal, because we can handle the next TDF */
                err = E_ERROR;
                goto error;
            }
        }
        else {
            /* TIRDB not in use -> set session_id to 0 */
            tdf.session_id = 0;
        }

        /* Now everything seems OK. Starting the benchmarks */
        if (storeResults == MODE_TO_TIRDB) {
            /* TIRDB is used */
            sprintf(msg,
                    "Starting session number %d '%s'",
                    tdf.session_id,
                    tdf.session_name);
        } else if (storeResults == MODE_TO_SQLFILE) {
            sprintf(msg, "Starting session '%s' and storing result data to sql file '%s'",
                    tdf.session_name, resultFileName);
        } else {
            sprintf(msg, "Starting session '%s'", tdf.session_name);
        }
        message('I', msg);

        archiveTestSessionLogs(&tdf, 0);

        /* Loop over all commands in the TDF */
        for (i = 0; i < num_of_bmrs; i++) {
            int tmp_storeResults = storeResults;

            bmrs[i]->test_run_id = 0;

            /* If TIRDB is in use, initialize TIRDB for benchmark data */
            setDateTimeNow(&(bmrs[i]->start_date), &(bmrs[i]->start_time));
            if (bmrs[i]->cmd_type == RUN) {
                /* Not for population */
                if (dedicatedThreads) {
                    bmrs[i]->cmd_type = RUN_DEDICATED;
                }
                if ( (storeResults == MODE_TO_SQLFILE)
                     || (storeResults == MODE_TO_TIRDB) ) {
                    /* If TIRDB is in use, initialize_tirdb_for_benchmark()
                       will change the value of bmrs[i]->test_run_id */
                    if (initialize_tirdb_for_benchmark(storeResults,
                                                       TIRDBConnectString,
                                                       resultFileName,
                                                       bmrs[i],
                                                       i,
                                                       &tdf) ) {
                        writeLog('E', "TIRDB initialization for benchmark failed");
                        /* Not fatal, because we can handle the next TDF */
                        err = E_ERROR;
                        goto error;
                    }
                }
            }

            if (bmrs[i]->cmd_type == EXECUTESQLFILE) {
                if (openFile(&fSQL, bmrs[i]->sql_file) != 0) {
                    sprintf(msg, "Cannot open SQL file '%s'",
                            bmrs[i]->sql_file);
                    message('E', msg);
                } else {
                    fclose(fSQL);
                    err = processSQLFile(bmrs[i]->sql_file, NULL, &server, ddf->db_connect);
                    if (err != 0) {
                        message('F',
                                "Error in SQL file processing");
                        err = E_FATAL;
                        goto error;
                    }
                }
            } else if (bmrs[i]->cmd_type == EXECUTESQL) {
                err = processSQL(bmrs[i]->sql_file, NULL, &server, ddf->db_connect);
                if (err != 0) {
                    message('F',
                            "Error in SQL processing");
                    err = E_FATAL;
                    goto error;
                }
            } else if (bmrs[i]->cmd_type == SLEEP) {
                sprintf(msg, "Sleeping for %d seconds ...",
                        bmrs[i]->run_duration);
                message('I', msg);
                msSleep(1000 * bmrs[i]->run_duration);
            } else {
                /* brms[i]->cmd_type == RUN */
                /* brms[i]->cmd_type == RUN_DEDICATED */
                /* brms[i]->cmd_type == POPULATE */
                /* brms[i]->cmd_type == POPULATE_CONDITIONALLY */

                if ((bmrs[i]->cmd_type == POPULATE)
                    || (bmrs[i]->cmd_type == POPULATE_CONDITIONALLY)
                    || (bmrs[i]->cmd_type == POPULATE_INCREMENTALLY)) {
                    storeResults = MODE_TO_LOGS_ONLY;
                }
                
                /* stop local server if it is started before running clients */
                if (server != NULL) {
                    err = stopServer(server);
                    if (err != 0) {
                        sprintf(msg, "Could not stop database server (%s), "
                                "error %d",
                                server_name,
                                err);
                        message('F', msg);
                        err = E_FATAL;
                        goto error;
                    }
                    server = NULL;
                }

                if (storeResults == MODE_TO_TIRDB) {
                    /* TIRDB is used */
                    sprintf(msg,
                            "Starting test run number %d '%s'",
                            bmrs[i]->test_run_id,
                            bmrs[i]->test_run_name);
                    writeLog('I', msg);
                } else if (strlen(bmrs[i]->test_run_name) > 0) {
                    sprintf(msg, "Starting test '%s'",
                            bmrs[i]->test_run_name);
                    writeLog('I', msg);
                }

                /* Start the Statistics process */
                err = spawnStatistics(&tdf, bmrs[i], storeResults,
                                      TIRDBConnectString, resultFileName);
                if (err) {
                    writeLog('E', "Starting of the Statistics process failed");
                } else {
                    /* Wait for the OK message from the Statistics */
                    if (receiveDataS(&g_comm, &senderId, &messageType, &data)) {
                        writeLog('E',
                                 "Error communicating with the "
                                 "Statistics module");
                        /* Fatal, stop execution*/
                        err = E_FATAL;
                        goto error;
                    }
                }
                /* create client processes to handle the benchmark runs,
                 * check the timings of the clients and finally send the
                 * request to start the test */
                if (!err) {
                    err = checkRemoteConnections(&tdf, bmrs[i]);
                    if (!err) {
                        err = pingRemotes(&tdf, bmrs[i]);
                    }
                    if (err) {
                        writeLog('E',
                                 "At least one remote control "
                                 "is not responding");
                    } else {
                        /* spawnClientsInNetwork also spawns the local clients
                         * running in the main node */
                        err = spawnClientsInNetwork(ddf,
                                                    &tdf,
                                                    bmrs[i],
                                                    workDirBase,
                                                    &mainClientProcesses,
                                                    ddf->db_schemaname,
                                                    waitDatabaseStart);
                        if (err) {
                            writeLog('E',
                                     "Starting of at least one Client process "
                                     "failed");
                        } else {
#ifdef ACCELERATOR
                            if (waitDatabaseStart > 0) {
                                /* wait for accelerator to load the database */
                                sprintf(msg, "Waiting %d seconds "
                                        "for the database "
                                        "to start up before getting "
                                        "answers from clients",
                                        waitDatabaseStart);
                                writeLog('I', msg);
                                msSleep(waitDatabaseStart*1000);
                                writeLog('I', "done");
                            } else if (waitDatabaseStart == 0) {
                                writeLog('I', "Press enter when "
                                         "the database has started.");
                                c = getchar();
                            }
#endif
                            /* database polling (waitDatabaseStart == -1) is
                               included in getClientResponses */
                            /* collect OK messages */
                            err = getClientResponses(&tdf,
                                                     bmrs[i],
                                                     waitDatabaseStart);
                        }
                    }

#ifdef ACCELERATOR
#ifdef CACHE_MODE
                    /* the frontend is now running, wait for connector
                       that needs to be started manually */
                    writeLog('I',
                             "Press enter when you are ready to start the "
                             "benchmark.");
                    c = getchar();
#endif
#endif
                    if (!err) {
                        /* Start the test timer here */
                        startTimer(&test_timer);
                        /* Give the test time to all the clients and make sure
                         * that the clients are well synchronized */
                        err = propagateTestTime(&test_timer,
                                                clientSynchThreshold,
                                                &tdf,
                                                bmrs[i]);

                        if (err) {
                            writeLog('E', "Client synchronization threshold "
                                     "exceeded");
                            writeLog('E',
                                     "Try adjusting 'tatp.ini' parameter "
                                     "'synchthreshold'");
                        }
                    }
                    if (!err) {
                        /* Send the test start request to all remotes and main
                         * node clients */
                        err = startTest(&tdf, bmrs[i]);
                        if (err) {
                            writeLog('E', "Could not start the test");
                        }
                    }
                    else {
                        interruptTest(&tdf, bmrs[i]);
                    }
                    if (!err) {
                        /* Wait for statistics message */
                        err = waitStatisticsMessage(bmrs[i]);
                        if (err) {
                            writeLog('E', "Statistics reported errors");
                        }
                    }

#ifndef WIN32
                    /* Release statistics process */
                    if (statistics_pid > 0) {
                        wpid = waitpid(statistics_pid, &status,
                                       WUNTRACED | WNOHANG);
                    }
#endif
                    /* Test run is now over. Collect the client log files from
                     * remote nodes and also from local clients */

                    collectTestRunLogs(workDirBase, &tdf, bmrs[i]);
                    /* main node log will be handled when the test session is
                     * finished  */

                    /* Test run is over, call "clean up" in remote nodes.
                     * This cleans up the local clients also (running in main
                     * node) */
                    if (finalizeTestInNetwork(&tdf, bmrs[i], mainClientProcesses)) {
                        writeLog('E', "Could not clean off all the clients.");
                    }
                }

                if ((bmrs[i]->cmd_type == POPULATE)
                    || (bmrs[i]->cmd_type == POPULATE_INCREMENTALLY)
                    || (bmrs[i]->cmd_type == POPULATE_CONDITIONALLY)) {
                    storeResults = tmp_storeResults;
                }
            } /* if (bmrs[i]->cmd_type == ... else { } */

            if (!err) {
                setDateTimeNow(&(bmrs[i]->stop_date), &(bmrs[i]->stop_time));
                if ((bmrs[i]->cmd_type == RUN)
                    || (bmrs[i]->cmd_type == RUN_DEDICATED)) {
                    /* Not for population */
                    sprintf(msg, "MQTh for test run is %d", bmrs[i]->avg_mqth);
                    writeLog('I', msg);
                    if ( (storeResults == MODE_TO_SQLFILE)
                         || (storeResults == MODE_TO_TIRDB) ) {
                        finalize_tirdb_for_benchmark(storeResults, TIRDBConnectString,
                                                     resultFileName, bmrs[i]);
                    }
                }
            }

            if (err) {
                break; /* Out of TDF */
            }
            /* wait desired time */
            if (bmrs[i]->cmd_type == POPULATE
                && bmrs[i]->post_population_delay > 0)
            {
                sprintf(msg,
                        "Post population delay of %d minutes",
                        bmrs[i]->post_population_delay);
                writeLog('I', msg);
                SLEEP(1000 * 60 * bmrs[i]->post_population_delay);
            }
        } /* end for */

        /* Benchmarks are handled. Free the memory allocated. */
        freeBenchmarks(bmrs, num_of_bmrs);

        /* If TIRDB is in use, finalize TIRDB for session (TDF) data */
        setDateTimeNow(&(tdf.stop_date), &(tdf.stop_time));
        if (storeResults == MODE_TO_TIRDB) {
            if (finalize_tirdb_for_session(TIRDBConnectString, &tdf)) {
                writeLog('E',
                         "TIRDB finalizing of session failed: "
                         "stop_date & stop_time will be missing");
                /* No return here, because this is a minor error */
            }
        }

        sprintf(msg,
                "Processing of TDF %s completed%s",
                tdfname,
                err ? " with errors" : "");
        message('I', msg);

        archiveTestSessionLogs(&tdf, 1);
        goto ok;

error:
        freeBenchmarks(bmrs, num_of_bmrs);
        return err;
ok:
        return 0;
}

/*##**********************************************************************\
 *
 *      readTDF
 *
 * Reads one TDF file and stores the content to dedicated data structure.
 *
 * Parameters:
 *      tdffilename
 *          The name of the tdf file
 *
 *      tdf
 *          The TDF data structure
 *
 *      bmrs
 *          The benchmark data structure
 *
 *      num_of_bmr
 *          The number of benchmarks defined in the TDF.
 *			Note that a populaton directive is counted as
 *			one benchmark in this case.
 *
 *      testSequence
 *          If not empty string, the test sequence from the commandline
 *          that replaces the test sequence in TDF
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int readTDF(char *tdffilename, struct tdfs *tdf, struct bmr *bmrs[],
            int *num_of_bmr, char *testSequence)
{
        FILE *fTDF = NULL;
        char line[W_L], msg[256];
        char tmp_testsequence[256];

        /* flag to indicate the first line in the TDF */
        int firstline;
        int retCode, transaction_num, client_distr_num;
        /* Flag type variables */
        int parsing_transaction_mix, parsing_client_distribution;
        enum tdf_file_sections tdf_file_section;
        int error, err;
        int repeats;

        parsing_transaction_mix = 0;
        parsing_client_distribution = 0;
        transaction_num = 0;
        client_distr_num = 0;
        error = err = 0;

        if (openFile(&fTDF, tdffilename) != 0) {
            sprintf(msg, "Cannot open TDF %s", tdffilename);
            writeLog('E', msg);
            /* Not fatal, because we can handle the next TDF */
            return E_ERROR;
        }
        sprintf(msg, "Test Definition File '%s'", tdffilename);
        writeLog('I', msg);

        initTDFDataStruct(tdf, 1);

        /* Save host name to the tdf struct */
        gethostname(tdf->control_host, W_L);
        /* Statistics and Main Control are in the same host */
        strncpy(tdf->statistics_host, tdf->control_host, W_L);

        firstline = 1;
        tdf_file_section = NONE;
        /* Iterate through the lines in TDF */
        while (readFileLine(fTDF, line, 256) != -1) {
            if (err == E_ERROR) {
                /* Handle the rest of the TDF and then exit with error */
                error = 1;
            }
            if (firstline) {
                firstline = 0;
                if (strncmp(line, "//tatp_tdf", 10) != 0) {
                    writeLog('E', "TDF has wrong or no identification line");
                    fclose(fTDF);
                    /* Not fatal, because we can handle the next TDF */
                    return E_ERROR;
                }
                continue; /* to next line */
            }
            /* Remove potential comments from the line (everything
             * after //) */
            removeComment(line);

            /* Skip blank lines */
            if (*line == '\0') {
                continue; /* to next line */
            }
            writeLog('D', line);

            /* Check if we hit the section change tag */
            retCode = isTDFSectionMarker(line, &tdf_file_section);
            if (retCode == 1) {
                /* Section change -> move to next line of TDF */
                if (parsing_transaction_mix) {
                    message('E',
                            "TDF section marker encountered while parsing "
                            "a transaction mix");
                    fclose(fTDF);
                    return E_ERROR;
                }
                if (parsing_client_distribution) {
                    message('E',
                            "TDF section marker encountered while parsing "
                            "a client distribution");
                    fclose(fTDF);
                    return E_ERROR;
                }
                if ( (tdf_file_section == TEST_SEQUENCE) && (testSequence[0] != '\0') ){
                    repeats = 0;
                    do {
                        strncpy(tmp_testsequence, testSequence, W_L);
                        err = parseTDFTestSequence(tmp_testsequence, tdf, bmrs, num_of_bmr, &repeats);
                    } while (repeats > 0);
                    break;
                }
                continue;
            }
            else if (retCode == -1) {
                message('E', "TDF section marker error");
                fclose(fTDF);
                /* Not fatal, because we can handle the next TDF */
                return E_ERROR;
            }
            switch (tdf_file_section) {
                case SESSION_PARAMETERS:
                    err = parseTDFSessionParameter(line, tdf);
                    continue;
                case POPULATION_PARAMETERS:
                    err = parseTDFPopulationParameter(line, tdf);
                    continue;
                case TEST_PARAMETERS:
                    err = parseTDFTestParameter(line, tdf);
                    continue;
                case TRANSACTION_MIXES:
                    err = parseTDFTransactionMixes(line, tdf,
                                                   &parsing_transaction_mix,
                                                   &transaction_num);
                    continue;
                case DATABASE_CLIENT_DISTRIBUTIONS:
                    err = parseTDFLoadDistributions(
                            line, tdf,
                            &parsing_client_distribution, &client_distr_num);
                    continue;
                case TEST_SEQUENCE:
                    repeats = 0;
                    do {
                        strncpy(tmp_testsequence, line, W_L);
                        err = parseTDFTestSequence(tmp_testsequence, tdf, bmrs, num_of_bmr, &repeats);
                    } while (repeats > 0);
                    continue;
                case NONE:
                    message('E', "A TDF directive before any section "
                            "marker in TDF");
                    err = E_ERROR;
                    continue;
            } /* switch (tdf_file_section) */

        } /* For the next line in TDF */

        fclose(fTDF);

        if (error == 1 || err == E_ERROR ||
			checkTDFData(tdf, bmrs, *num_of_bmr, tdffilename) == E_ERROR) {
            sprintf(msg, "Errors in TDF (%s)", tdffilename);
            writeLog('E', msg);
            /* Not fatal, because we can handle the next TDF */
            return E_ERROR;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      initDDFDataStruct
 *
 * Initializes the DDF data structure. Use default values when appropriate.
 *
 * Parameters:
 *      ddf
 *          DDF data structure
 *
 * Return value:
 *      none
 */
void initDDFDataStruct(struct ddfs *ddf) {
        *ddf->db_name = '\0';
        *ddf->db_version = '\0';
        *ddf->db_connect = '\0';
        *ddf->os_name = '\0';
        *ddf->os_version = '\0';
        *ddf->hardware_id = '\0';
        *ddf->configuration_file_name = '\0';
        *ddf->configuration_code = '\0';
        *ddf->configuration_comments = '\0';
        *ddf->configuration_file_contents = '\0';

        strcpy(ddf->db_schemafile, DEFAULT_DBSCHEMAFILE_NAME);
        strcpy(ddf->db_transactionfile, DEFAULT_TRANSACTIONFILE_NAME);
        *ddf->db_initfile = '\0';
        *ddf->db_connect_initfile = '\0';
        *ddf->db_schemaname = '\0';
}

/*##**********************************************************************\
 *
 *      initTDFDataStruct
 *
 * Initializes the TDF data structure. Use default values when appropriate.
 *
 * Parameters:
 *      tdf
 *          TDF data structure
 *
 * Return value:
 *      none
 */
void initTDFDataStruct(struct tdfs *tdf, int defaultvalues)
{
        int i, j;
        /*  Session parameters */
        tdf->session_name[0] = '\0';
        tdf->author[0] = '\0';
        tdf->comments[0] = '\0';
        /* Population parameters */
        tdf->subscribers = DEFAULT_NUM_OF_SUBSCRIBERS;
        tdf->serial_keys = DEFAULT_SERIAL_KEY_MODE;
        tdf->commitblock_size = DEFAULT_COMMIT_BLOCK_SIZE;
        tdf->post_population_delay = DEFAULT_POST_POPULATION_DELAY;
        tdf->check_targetdb = DEFAULT_CHECK_TARGETDB;
        /* Test parameters */
        if (defaultvalues) {
            tdf->warm_up_duration = DEFAULT_WARM_UP_DURATION;
            tdf->run_duration = DEFAULT_RUN_DURATION;
            tdf->uniform = DEFAULT_UNIFORM;
            tdf->throughput_resolution = DEFAULT_THROUGHPUT_RESOLUTION;
            tdf->repeats = 1;
        } else {
            tdf->warm_up_duration = UNDEFINED_VALUE;
            tdf->run_duration = UNDEFINED_VALUE;
            tdf->uniform = DEFAULT_UNIFORM;
            tdf->throughput_resolution = DEFAULT_THROUGHPUT_RESOLUTION;
            tdf->repeats = UNDEFINED_VALUE;
        }

        /* Transaction mixes */
        for (i = 0; i < MAX_NUM_OF_TRANSACTION_MIXES; i++) {
            tdf->tr_mixes[i].name[0] = '\0';
            for (j = 0; j < MAX_NUM_OF_TRANSACTIONS; j++) {
                tdf->tr_mixes[i].tr_props[j].transact[0] = '\0';
                tdf->tr_mixes[i].tr_props[j].prob = 0;
            }
        }
        tdf->num_of_tr_mixes = 0;
        /* Load distributions */
        for (i = 0; i < MAX_NUM_OF_CLIENT_DISTRIBUTIONS; i++) {
            tdf->client_distributions[i].name[0] = '\0';
            for (j = 0; j < MAX_NUM_OF_REMOTE_COMPUTERS; j++) {
                tdf->client_distributions[i].
                    rem_loads[j].remControls_index = 0;
                tdf->client_distributions[i].
                    rem_loads[j].remLoad = 0;
                tdf->client_distributions[i].
                    rem_loads[j].remLoadProcesses = DEFAULT_CLIENT_PROCESSES;
            }
        }
        tdf->num_of_client_distributions = 0;
}

/*##**********************************************************************\
 *
 *      checkTDFData
 *
 * Validates the TDF data that was read from the TDF file.
 *
 * Parameters:
 *      tdf
 *          TDF data structure
 *
 *      bmrs
 *			Benchmark run time data structure
 *
 *      num_of_bmr
 *			Number of bmrs in the structure
 *
 *      tdffilename
 *			TDF file name
 *
 * Return value:
 *       0 - success
 *      !0 - data not ok
 */
int checkTDFData(struct tdfs *tdf, struct bmr *bmrs[], int num_of_bmr,
                 char *tdffilename)
{
        int i, j, err;
        char msg[256];
        err = 0;

        /* Population parameters */
        if (tdf->subscribers < 1) {
            sprintf(msg, "TDF (%s) Population parameter: 'subscribers' "
                    "has to be >0",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        if (tdf->commitblock_size < 0) {
            sprintf(msg, "TDF (%s) Population parameter: 'commit_block_rows' "
                    "has to be >=0",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        if (tdf->post_population_delay < 0) {
            sprintf(msg, "TDF (%s) Population parameter: "
                    "'post_population_delay' "
                    "has to be >=0",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        /* Test parameters */
        if (tdf->warm_up_duration < 0) {
            sprintf(msg, "TDF (%s) Test parameter: 'warm_up_duration' "
                    "has to be >=0",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        if (tdf->run_duration < 1) {
            sprintf(msg, "TDF (%s) Test parameter: 'run_duration' "
                    "has to be >0",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        if (tdf->repeats < 1) {
            sprintf(msg, "TDF (%s) Test parameter: 'repeats' "
                    "has to be >0",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        if (tdf->uniform != 0
            && tdf->uniform != 1) {
            sprintf(msg, "TDF (%s) Test parameter: 'uniform' "
                    "has to be one of [0, 1]",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        if (tdf->throughput_resolution < 1) {
            sprintf(msg, "TDF (%s) Test parameter: "
                    "'throughput_resolution' "
                    "has to be >0",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        /* Check the run time parameters also */
        for (i = 0; i < num_of_bmr; i++) {

            switch (bmrs[i]->cmd_type) {
                case EXECUTESQL:
                case EXECUTESQLFILE:
                case SLEEP:
                case NOP:
                    break;
                default:
                    /* Check that load distribution reference exist */
                    for (j = 0; j < tdf->num_of_client_distributions; j++) {
                        if (strncmp(bmrs[i]->client_distribution_str,
                                    tdf->client_distributions[j].name, W) == 0) {
                            /* Client load distribution with the given name was
                               found  */
                            bmrs[i]->client_distribution_ind = j;
                            break;
                        }
                    }
                    if (j == tdf->num_of_client_distributions) {
                        /* A client load distribution with a given name
                           is not defined */
                        sprintf(msg, "TDF (%s) 'run' command: "
                                "unresolved db client "
                                "distribution name '%s'",
                                tdffilename, bmrs[i]->client_distribution_str);
                        writeLog('E', msg);
                        err = 1;
                    }
            }
            switch (bmrs[i]->cmd_type) {
                case POPULATE_INCREMENTALLY:
                case POPULATE_CONDITIONALLY:
                case POPULATE:
                    if (bmrs[i]->subscribers < 1) {
                        sprintf(msg, "TDF (%s) 'populate' command: "
                                "'subscribers' "
                                "has to be >0",
                                tdffilename);
                        writeLog('E', msg);
                        err = 1;
                    }
                    if (bmrs[i]->commitblock_size < 0) {
                        sprintf(msg, "TDF (%s) 'populate' command: "
                                "'commit_block_rows' "
                                "has to be >=0",
                                tdffilename);
                        writeLog('E', msg);
                        err = 1;
                    }
                    if (bmrs[i]->min_subscriber_id < 1) {
                        sprintf(msg, "TDF (%s) 'populate' command: "
                                "'min_subscriber_id' "
                                "has to be >=1",
                                tdffilename);
                        writeLog('E', msg);
                        err = 1;
                    }
                    if (bmrs[i]->post_population_delay < 0) {
                        sprintf(msg, "TDF (%s) 'populate' command: "
                                "'post_population_delay' has to be >=0",
                                tdffilename);
                        writeLog('E', msg);
                        err = 1;
                    }
                    break;
                case RUN:
                case RUN_DEDICATED:
                    if (bmrs[i]->run_duration < 1) {
                        sprintf(msg, "TDF (%s) 'run' command: 'run_duration' "
                                "has to be >0",
                                tdffilename);
                        writeLog('E', msg);
                        err = 1;
                    }
                    if (bmrs[i]->repeats < 1) {
                        sprintf(msg, "TDF (%s) 'run' command: 'repeats' "
                                "has to be >0",
                                tdffilename);
                        writeLog('E', msg);
                        err = 1;
                    }
                    if (bmrs[i]->warm_up_duration < 0) {
                        sprintf(msg, "TDF (%s) 'run' command: 'warm_up_duration' "
                                "has to be >=0",
                                tdffilename);
                        writeLog('E', msg);
                        err = 1;
                    }
                    /* Check that transaction mix reference exist */
                    for (j = 0; j < tdf->num_of_tr_mixes; j++) {
                        if (strncmp(bmrs[i]->transaction_mix_str,
                                    tdf->tr_mixes[j].name, W) == 0) {
                            /* Transaction mix with the given name was found  */
                            bmrs[i]->transaction_mix_ind = j;
                            break;
                        }
                    }
                    if (j == tdf->num_of_tr_mixes) {
                        /* A transaction mix with a given name is not defined */
                        sprintf(msg, "TDF (%s) 'run' command: unresolved "
                                "transaction mix name '%s'",
                                tdffilename, bmrs[i]->transaction_mix_str);
                        writeLog('E', msg);
                        err = 1;
                    }
                    break;
                default:
                    break;
            }
        }
        if (err == 1) {
            /* Not fatal, because we can handle the next TDF */
            return E_ERROR;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      isTDFSectionMarker
 *
 * Checks if a character string given as an argument
 * has a TDF section starter tag.
 *
 * Parameters:
 *      line
 *          the character string
 *
 *      tdf_file_section
 *          the name of the tdf section
 *
 * Return value:
 *      1  - tag found
 *      0  - tag not found
 *     <0  - error (section tag is not the only thing in the string)
 */
int isTDFSectionMarker(const char *line,
                       enum tdf_file_sections *tdf_file_section)
{
        if (strstr(line, "[Session parameters]")) {
            if (strlen(line) > 20) return -1;
            *tdf_file_section = SESSION_PARAMETERS;
            return 1;
        }
        else if (strstr(line, "[Population parameters]")) {
            if (strlen(line) > 23) return -1;
            *tdf_file_section = POPULATION_PARAMETERS;
            return 1;
        }
        else if (strstr(line, "[Test parameters]")) {
            if (strlen(line) > 17) return -1;
            *tdf_file_section = TEST_PARAMETERS;
            return 1;
        }
        else if (strstr(line, "[Transaction mixes]")) {
            if (strlen(line) > 19) return -1;
            *tdf_file_section = TRANSACTION_MIXES;
            return 1;
        }
        else if (strstr(line, "[Database client distributions]")) {
            if (strlen(line) > 31) return -1;
            *tdf_file_section = DATABASE_CLIENT_DISTRIBUTIONS;
            return 1;
        }
        else if (strstr(line, "[Test sequence]")) {
            if (strlen(line) > 15) return -1;
            *tdf_file_section = TEST_SEQUENCE;
            return 1;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      parseTDFSessionParameter
 *
 * Parses one line from the TDF file that is within the
 * SESSION_PARAMETERS section.
 *
 * Parameters:
 *      line
 *          the character string
 *
 *      tdf
 *          the test definition data structure
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int parseTDFSessionParameter(const char *line, struct tdfs *tdf)
{
        char msg[256];
        if ((extractStringKeyword(line, "session_name",
                                  tdf->session_name, W_L))
            != E_NO_KEYWORD) {
			return 0;
        }
        if ((extractStringKeyword(line, "author", tdf->author, W))
            != E_NO_KEYWORD) {
			return 0;
        }
        if ((extractIntKeyword(line, "throughput_resolution",
                               &tdf->throughput_resolution)) != E_NO_KEYWORD) {
            return 0;
        }
        if ((extractStringKeyword(line, "comments", tdf->comments, W_L))
            != E_NO_KEYWORD) {
			return 0;
        }
        sprintf(msg,
                "Unknown directive in [Session parameters] "
                "section in TDF (%s).",
                line);
        message('E', msg);
        return E_ERROR;
}

/*##**********************************************************************\
 *
 *      parseTDFPopulationParameter
 *
 * Parses one line from the TDF file that is within the
 * POPULATION_PARAMETERS section.
 *
 * Parameters:
 *      line
 *          the character string
 *
 *      tdf
 *          the test definition data structure
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int parseTDFPopulationParameter(const char *line, struct tdfs *tdf)
{
        char msg[256];
        char value[W];
        if ((extractIntKeyword(line, "subscribers", &tdf->subscribers))
            != E_NO_KEYWORD) {
            return 0;
        }
        if ((extractStringKeyword(line, "serial_keys", value, W))
            != E_NO_KEYWORD) {
			if (strncmp(value, "yes", 3) == 0) {
				tdf->serial_keys = 1;
				return 0;
			}
			else if (strncmp(value, "no", 2) == 0) {
				tdf->serial_keys = 0;
				return 0;
			}
			else {
				sprintf(msg, "Wrong value for 'serial_keys' in "
                        "[Population parameters] section in TDF.");
				message('E', msg);
				return E_ERROR;
			}
        }
        if ((extractStringKeyword(line, "check_targetdb", value, W))
            != E_NO_KEYWORD) {
			if (strncmp(value, "yes", 3) == 0) {
				tdf->check_targetdb = 1;
				return 0;
			}
			else if (strncmp(value, "no", 2) == 0) {
				tdf->check_targetdb = 0;
				return 0;
			}
			else {
				sprintf(msg, "Wrong value for 'check_targetdb' in "
                        "[Population parameters] section in TDF.");
				message('E', msg);
				return E_ERROR;
			}
        }
        if ((extractIntKeyword(line, "commit_block_rows",
                               &tdf->commitblock_size)) != E_NO_KEYWORD) {
            return 0;
        }
        if ((extractIntKeyword(line, "post_population_delay",
                               &tdf->post_population_delay)) != E_NO_KEYWORD) {
            return 0;
        }
        sprintf(msg,
                "Unknown directive (or value) in "
                "[Population parameters] section in TDF (%s).",
                line);
        message('E', msg);
        return E_ERROR;
}

/*##**********************************************************************\
 *
 *      parseTDFTestParameter
 *
 * Parses one line from the TDF file that is within the
 * TEST_PARAMETERS section.
 *
 * Parameters:
 *      line
 *          the character string
 *
 *      tdf
 *          the test definition data structure
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int parseTDFTestParameter(const char *line, struct tdfs *tdf)
{
/* todo: use parameters from cmd line */

        char msg[256];
        char buf[W];
        if ((extractIntKeyword(line, "warm_up_duration",
                               &tdf->warm_up_duration)) != E_NO_KEYWORD) {
            return 0;
        }
        if ((extractIntKeyword(line, "run_duration",
                               &tdf->run_duration)) != E_NO_KEYWORD) {
            return 0;
        }
        if ((extractIntKeyword(line, "repeats",
                               &tdf->repeats)) != E_NO_KEYWORD) {
            return 0;
        }

        if ((extractStringKeyword(line, "check_targetdb", buf, W))
            != E_NO_KEYWORD) {
            if (strncmp(buf, "yes", 3) == 0) {
                tdf->check_targetdb = 1;
                return 0;
            }
            else if (strncmp(buf, "no", 2) == 0) {
                tdf->check_targetdb = 0;
                return 0;
            }
            else {
                sprintf(msg, "Wrong value for 'check_targetdb' in "
                        "[Test parameters] section in TDF, "
                        "must be \"yes\"/\"no\".");
                message('E', msg);
                return E_ERROR;
            }
        }
        if ((extractStringKeyword(line, "uniform",
                                  buf, W)) != E_NO_KEYWORD) {

            if (strcmp(buf, "yes") == 0) {
                tdf->uniform = 1;
            }
            else if (strcmp(buf, "no") == 0) {
                tdf->uniform = 0;
            }
            else {
                sprintf(msg, "Wrong value for 'uniform' in "
                        "[Test parameters] section in TDF, must be "
                        "\"yes\"/\"no\".");
                message('E', msg);
                return E_ERROR;
            }

            return 0;
        }
        sprintf(msg, "Unknown test directive '%s'.", line);
        message('E', msg);
        return E_ERROR;
}

/*##**********************************************************************\
 *
 *      parseTDFTransactionMixes
 *
 * Parses one line from the TDF file that is within the
 * TRANSACTION_MIXES section.
 *
 * Parameters:
 *      line
 *          the character string
 *
 *      tdf
 *          the test definition data structure
 *
 *      parsing_transaction_mix
 *          a flag indicating the parsing status (whether
 *          we are currently parsing a transaction mix or not)
 *
 *      transaction_num
 *          The transaction number within a transaction mix
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int parseTDFTransactionMixes(const char *line, struct tdfs *tdf,
							 int *parsing_transaction_mix,
							 int *transaction_num) {
        char msg[256];
        int i, propability_count;
        /* regular expressions */
        regex_t *r;
        /* used with regex to locate the string hits */
        int count, starts[100], lengths[100];

        r = (regex_t *) malloc(sizeof(regex_t));
        if (r == NULL) {
            writeLog('E', "Cannot reserve memory for regex_t");
            return E_ERROR;
        }
        if (!(*parsing_transaction_mix)) {
            /* We are not currently parsing a mix */
            /* Try to match for the transaction mix start "name = {" */
            if (!multiMatch(r, (char*)line, "^ *(.*) *= *\\{ *$", &count,
                            starts, lengths)) {
                if (tdf->num_of_tr_mixes >= MAX_NUM_OF_TRANSACTION_MIXES) {
                    sprintf(msg,
                            "More than MAX_NUM_OF_TRANSACTION_MIXES (%d) "
                            "transaction mixes "
                            "defined in TDF (%s).",
                            MAX_NUM_OF_TRANSACTION_MIXES, line);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
                /* Extract the name of the transaction mix */
                strncat(tdf->tr_mixes[tdf->num_of_tr_mixes].name,
                        &line[starts[1]], minimum(lengths[1]-1,W));
                *parsing_transaction_mix = 1;
                *transaction_num = 0;
            }
        }
        else if (!multiMatch(r, (char*)line, "^ *\\} *$", &count,
                             starts, lengths)) {
            /* Leaving the transaction mix block */
            *parsing_transaction_mix = 0;
            /* Check that the propabilities count to 100% */
            propability_count = 0;
            for (i = 0; i < *transaction_num; i++) {
                propability_count +=
                    tdf->tr_mixes[tdf->num_of_tr_mixes].tr_props[i].prob;
            }
            if (propability_count != 100) {
				sprintf(msg,
                        "Probability in the transaction mix %s "
                        "does not sum to 100.",
                        tdf->tr_mixes[tdf->num_of_tr_mixes].name);
				writeLog('E', msg);
				free(r);
				return E_ERROR;
            }
            (tdf->num_of_tr_mixes)++;
        }
        else {
            /* Currently parsing a mix (we are within the { } block) */
            /* Try to match for a transaction name and its probability */
            if (!multiMatch(r, (char*)line, "^ *(.*) +([0-9]+) *$",
                            &count, starts, lengths)) {
                if (*transaction_num >= MAX_NUM_OF_TRANSACTIONS) {
                    sprintf(msg,
                            "More than %d transactions defined "
                            "in a transaction "
                            "mix in TDF (%s).",
                            MAX_NUM_OF_TRANSACTIONS, line);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
                if (count != 3) {
                    sprintf(msg, "Transaction / propability -pair error in "
                            "TDF file (%s)",
                            line);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
                /* Assing the transaction name and its probability */
                strncat(tdf->tr_mixes[tdf->num_of_tr_mixes].
                        tr_props[*transaction_num].transact,
                        &line[starts[1]], minimum(lengths[1],W_L));
                tdf->tr_mixes[tdf->num_of_tr_mixes].
                    tr_props[*transaction_num].prob = atoi(&line[starts[2]]);
                (*transaction_num)++;
            }
        }
        free(r);
        return 0;
}

/*##**********************************************************************\
 *
 *      parseTDFLoadDistributions
 *
 * Parses one line from the TDF file that is within the
 * LOAD_DISTRIBUTIONS section.
 *
 * Parameters:
 *      line
 *          the character string
 *
 *      tdf
 *          the test definition data structure
 *
 *      parsing_client_distribution
 *          flag indicating whether we are currently parsing
 *          a client distribution
 *
 *      client_num
 *          number of the current client distribution
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int parseTDFLoadDistributions(const char *line, struct tdfs *tdf,
                              int *parsing_client_distribution,
                              int *client_num) {
        char msg[256], client_name[W_L];
        int i, client_count;
        /* regular expressions */
        regex_t *r;
        /* used with regex to locate the string hits */
        int count, starts[100], lengths[100];
        char *strptr = NULL;
        char strClients[W];

        r = (regex_t *) malloc(sizeof(regex_t));
        if (r == NULL) {
            writeLog('E', "Cannot reserve memory for regex_t");
            return E_ERROR;
        }
        if (!(*parsing_client_distribution)) {
            /* We are not currently parsing a load distribution */
            /* Try to match for the load distribution start "name = {" */
            if (!multiMatch(r, (char*)line, "^ *(.*) *= *\\{ *$",
                            &count, starts, lengths)) {
                if (tdf->num_of_client_distributions
                    == MAX_NUM_OF_CLIENT_DISTRIBUTIONS) {
                    sprintf(msg,
                            "More than MAX_NUM_OF_CLIENT_DISTRIBUTIONS (%d) "
                            "load distributions defined "
                            "in TDF.",
                            MAX_NUM_OF_CLIENT_DISTRIBUTIONS);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
                /* Extract the name of the load distribution */
                strncat(tdf->client_distributions[
                                tdf->num_of_client_distributions].name,
                        &line[starts[1]], minimum(lengths[1]-1,W));
                *parsing_client_distribution = 1;
                *client_num = 0;
                tdf->client_distributions[tdf->num_of_client_distributions].
                    localLoad = 0;
                tdf->client_distributions[tdf->num_of_client_distributions].
                    localLoadProcesses = DEFAULT_CLIENT_PROCESSES;
            }
        }
        else if (!multiMatch(r, (char*)line, "^ *\\} *$", &count,
                             starts, lengths)) {
            /* Leaving the load distribution block */
            *parsing_client_distribution = 0;
            /* Check that the number of db clients does not exceed
             * MAX_CLIENTS */
            client_count = tdf->client_distributions[
                    tdf->num_of_client_distributions].localLoad;
            if (tdf->client_distributions[
                        tdf->num_of_client_distributions].localLoadProcesses
                > client_count) {
                sprintf(msg,
                        "More client processes (%d) than clients (%d) defined for "
                        "'localhost' in client distribution '%s'",
                        tdf->client_distributions[
                                tdf->num_of_client_distributions].localLoadProcesses,
                        client_count,
                        tdf->client_distributions[
                                tdf->num_of_client_distributions].name);
				writeLog('E', msg);
				free(r);
				return E_ERROR;
            }
            for (i = 0; i < *client_num; i++) {
                client_count +=
                    tdf->client_distributions[
                            tdf->num_of_client_distributions].
                            rem_loads[i].remLoad;
                if (tdf->client_distributions[
                            tdf->num_of_client_distributions].
                            rem_loads[i].remLoad <
                    tdf->client_distributions[
                            tdf->num_of_client_distributions].
                    rem_loads[i].remLoadProcesses) {
                    sprintf(msg,
                            "More client processes (%d) than clients (%d) "
                            "defined for a Remote Node in client "
                            "distribution '%s'",
                            tdf->client_distributions[
                                    tdf->num_of_client_distributions].
                            rem_loads[i].remLoadProcesses,
                            tdf->client_distributions[
                                    tdf->num_of_client_distributions].
                            rem_loads[i].remLoad,
                            tdf->client_distributions[
                                    tdf->num_of_client_distributions].name);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
            }
            if (client_count > MAX_CLIENTS) {
				sprintf(msg,
                        "More than %d database clients defined in %s.",
                        MAX_CLIENTS, tdf->client_distributions[
                                tdf->num_of_client_distributions].name);
				writeLog('E', msg);
				free(r);
				return E_ERROR;
            }
            (tdf->num_of_client_distributions)++;
        }
        else {
            /* Currently parsing a distribution (we are within the { }
             * block) */
            /* Try to match for a client name and its load */
            if ((!multiMatch(r, (char*)line,
                             "^ *([-a-zA-Z0-9\\.]*) +([0-9\\/]+) *$",
                             &count, starts, lengths))
                || (!multiMatch(r, (char*)line,
                                "^ *([-a-zA-Z0-9\\.]*) +([0-9\\/]+) +"
                                "([0-9]+) +([0-9]+) *$",
                                &count, starts, lengths))) {

                if (*client_num >= MAX_NUM_OF_REMOTE_COMPUTERS) {
                    sprintf(msg,
                            "More than %d client computers defined "
                            "in a db client "
                            "distribution in TDF (%s).",
                            MAX_NUM_OF_REMOTE_COMPUTERS, line);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
                if ((count != 3) && (count != 5)) {
                    sprintf(msg, "Client / load -pair error in TDF file (%s)",
                            line);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
                /* Assign the client name and its load */
                /* First find appropriate client from client machine
                 * structure */
                client_name[0] = '\0';
                strncat(client_name, &line[starts[1]],
                        minimum(lengths[1],W_L));
                if (strncmp(client_name, "localhost", 9) == 0) {
                    /* Special case when we use the local computer */
                    strClients[0] = '\0';
                    strncat(strClients, &line[starts[2]],
                            minimum(lengths[2], W));
                    strptr = strstr (strClients, "/");

                    if (strptr != NULL) {
                        /* specified amount of processes */
                        tdf->client_distributions[tdf->num_of_client_distributions].
                            localLoadProcesses = atoi(strptr+1);
                        *strptr = '\0';
                    }

                    tdf->client_distributions[tdf->num_of_client_distributions].
                        localLoad = atoi(strClients);

                    tdf->client_distributions[tdf->num_of_client_distributions].
                        min_subs_id = 0;
                    tdf->client_distributions[tdf->num_of_client_distributions].
                        max_subs_id = 0;
                    /* S_ID range */
                    if (count == 5) {
                        tdf->client_distributions[
                                tdf->num_of_client_distributions].
                                    min_subs_id = atoi(&line[starts[3]]);
                        tdf->client_distributions[
                                tdf->num_of_client_distributions].
                                    max_subs_id = atoi(&line[starts[4]]);
                    }
                    free(r);
                    return 0;
                }
                else {
                    for (i = 1; i < MAX_NUM_OF_REMOTE_COMPUTERS; i++) {
                        if (strncmp(client_name,
                                    remControls[i].name, W_L) == 0) {
                            /* then store the index of the machine in
                               the client machine structure */
                            tdf->client_distributions[
                                    tdf->num_of_client_distributions].
                                rem_loads[*client_num].remControls_index = i;
                            break;
                        }
                    }
                }
                if (i == MAX_NUM_OF_REMOTE_COMPUTERS) {
                    printf(msg, "Client computer '%s' used in TDF file is "
                            "not defined",
                            client_name);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
                /* Assign the client load */
                strClients[0] = '\0';
                strncat(strClients, &line[starts[2]],
                        minimum(lengths[2], W));
                strptr = strstr (strClients, "/");

#ifdef ACCELERATOR
		tdf->client_distributions[tdf->num_of_client_distributions].
		  rem_loads[*client_num].remLoadProcesses = 1;
#else
                if (strptr != NULL) {
                    /* specified amount of processes */
                    tdf->client_distributions[tdf->num_of_client_distributions].
                        rem_loads[*client_num].remLoadProcesses = atoi(strptr+1);
                    *strptr = '\0';
                }
#endif
                tdf->client_distributions[tdf->num_of_client_distributions].
                    rem_loads[*client_num].remLoad = atoi(strClients);

                tdf->client_distributions[tdf->num_of_client_distributions].
                    rem_loads[*client_num].min_subs_id=0;
                tdf->client_distributions[tdf->num_of_client_distributions].
                    rem_loads[*client_num].max_subs_id=0;
                /* S_ID range */
                if (count == 5) {
                    tdf->client_distributions[tdf->num_of_client_distributions].
                        rem_loads[*client_num].min_subs_id = atoi(
                                                             &line[starts[3]]);
                    tdf->client_distributions[tdf->num_of_client_distributions].
                        rem_loads[*client_num].max_subs_id = atoi(
                                                             &line[starts[4]]);
                }
                (*client_num)++;
            } else {
                sprintf(msg,
                        "Unknown row '%s' in database client distribution",
                        line);
                writeLog('E', msg);
                free(r);
                return E_ERROR;
            }
        }
        free(r);
        return 0;
}

/*##**********************************************************************\
 *
 *      parseTDFTestSequence
 *
 * Parses one line from the TDF file that is within the TEST_SEQUENCE
 * section. Two parameters are expected: transaction_mix and
 * database_client_distribution.  The 'test_parameters' may also exist
 * (optional). Each 'test_parameter' not given is assigned the value
 * given in the [Test parameters] section (or default value if the
 * parameters is not defined at all).
 *
 * The parameter values  may also be replaced with the values defined
 * in the command line, thus the order is:
 * 1. defaults
 * 2. [Test parameter] section
 * 3. [Test sequence] line
 * 4. -x option from the command line
 *
 * Parameters:
 *      line
 *          the character string
 *
 *      tdf
 *          the test definition data structure
 *
 *      bm_run
 *          benchmark run(s) data structure
 *
 *      num_of_bmr
 *          number of benchmark runs defined
 *
 *      repeats
 *          number of repeats counter
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int parseTDFTestSequence(char *line, struct tdfs *tdf,
                         struct bmr *bm_run[], int *num_of_bmr, int *repeats)
{
        char msg[256], *c;
        char strValue[W];
        int tmp_num_of_bmr;
        enum test_sequence_cmd_type operationType;
        if (*num_of_bmr >= MAX_BM) {
            sprintf(msg, "More than %d commands in the [Test sequence] "
                    "section in TDF (%s).", MAX_BM, line);
            message('E', msg);
            return E_ERROR;
        }
        /* Reserve memory for a benchmark run structure */
        bm_run[*num_of_bmr] = (struct bmr*)malloc(sizeof(struct bmr));
        if (!bm_run[*num_of_bmr]) {
            message('E', "Could not reserve memory for benchmark data "
                    "structure");
            return E_ERROR;
        }

        /* Set inital values for some of the run parameters */
        /* based on TDF file / command line */
        initBMRunParameters(tdf, bm_run[*num_of_bmr]);

        for (c = line; *c == ' '; c++) ;
        digestBasicOperationType(c, &operationType);
        switch(operationType) {
            case POPULATE:
            case POPULATE_CONDITIONALLY:
            case POPULATE_INCREMENTALLY:
                bm_run[*num_of_bmr]->cmd_type = operationType;

                if (!strstr(line, "database_client_distribution")) {
                    if ((tdf_cmdline == NULL)
                        || ((tdf_cmdline != NULL)
                            && (*(tdf_cmdline->client_distributions[0].name)
                                == '\0'))) {

                        if (tdf->num_of_client_distributions
                            < MAX_NUM_OF_CLIENT_DISTRIBUTIONS) {
                            strncpy(tdf->client_distributions[
                                            tdf->num_of_client_distributions].
                                            name, "default_dcd", W);
                            tdf->client_distributions[
                                    tdf->num_of_client_distributions].
                                            localLoad = 1;
                            tdf->client_distributions[
                                    tdf->num_of_client_distributions].
                                            localLoadProcesses = 1;
                            tdf->client_distributions[
                                    tdf->num_of_client_distributions].
                                            min_subs_id = 0;
                            tdf->client_distributions[
                                    tdf->num_of_client_distributions].
                                            max_subs_id = 0;
                            tdf->num_of_client_distributions++;

                            strncpy(bm_run[*num_of_bmr]->
                                    client_distribution_str, "default_dcd", W);
                        } else {
                            sprintf(msg,
                                    "More than MAX_NUM_OF_CLIENT_DISTRIBUTIONS "
                                    "(%d) load distributions defined.",
                                    MAX_NUM_OF_CLIENT_DISTRIBUTIONS);
                            message('E', msg);
                            return E_ERROR;
                        }
                    }
                }

                /* Extract the keywords from the command */
                extractIntKeyword(line, "subscribers",
                                  &(bm_run[*num_of_bmr]->subscribers));

                /* replace with cmd line parameters if defined */
                if (tdf_cmdline != NULL) {
                    if (tdf_cmdline->subscribers != UNDEFINED_VALUE) {
                        bm_run[*num_of_bmr]->subscribers =
                            tdf_cmdline->subscribers;
                    }
                }

                extractStringKeyword(line, "database_client_distribution",
                                     bm_run[*num_of_bmr]->
                                     client_distribution_str, W_L);
                if (tdf_cmdline != NULL) {
                    if (*(tdf_cmdline->client_distributions[0].name) != '\0') {
                        strncpy(bm_run[*num_of_bmr]->client_distribution_str,
                                tdf_cmdline->client_distributions[0].name, W);
                    }
                }

                if (extractStringKeyword(line, "serial_keys",
                                         strValue, W) == 0) {
                    if (strncmp(strValue, "yes", 3) == 0) {
                        bm_run[*num_of_bmr]->serial_keys = 1;
                    }
                    else if (strncmp(strValue, "no", 2) == 0) {
                        bm_run[*num_of_bmr]->serial_keys = 0;
                    }
                    else {
                        sprintf(msg, "Unknown value for 'populate' command in TDF.");
                        message('E', msg);
                        return E_ERROR;
                    }
                }
                extractStringKeyword(line, "name",
                                     bm_run[*num_of_bmr]->test_run_name, W_L); 
                extractIntKeyword(line, "min_subscriber_id",
                                  &(bm_run[*num_of_bmr]->min_subscriber_id));
                extractIntKeyword(line, "commit_block_rows",
                                  &(bm_run[*num_of_bmr]->commitblock_size));
                extractIntKeyword(line, "post_population_delay",
                                  &(bm_run[*num_of_bmr]->
                                    post_population_delay));
                if (!isEmptyBuf(line, strlen(line))) {
                    sprintf(msg, "Unknown content (%s) in table populate "
                            "command in TDF.",
                            line);
                    message('E', msg);
                    return E_ERROR;
                }
                break;
            case RUN:
            case RUN_DEDICATED:
                /* mandatory parameters */
                if (!strstr(line, "transaction_mix")) {
                    if ((tdf_cmdline == NULL) ||
                        ((tdf_cmdline != NULL) && (*(tdf_cmdline->tr_mixes[0].name) == '\0'))) {
                        sprintf(msg, "Transaction mix not defined for 'run' "
                                "command in TDF");
                        message('E', msg);
                        return E_ERROR;
                    }
                }
                if (!strstr(line, "database_client_distribution")) {
                    if ((tdf_cmdline == NULL) ||
                        ((tdf_cmdline != NULL) && (*(tdf_cmdline->client_distributions[0].name) == '\0'))) {
                        sprintf(msg, "Database client distribution not "
                                "defined for 'run' command in TDF");
                        message('E', msg);
                        return E_ERROR;
                    }
                }

                bm_run[*num_of_bmr]->cmd_type = RUN;

                /* Verify the 'subscribers' value. If we had one or many
                   'populate' command(s) in the test sequence before this RUN
                   command, then use the 'subscribers' from the latest
                   'populate' command. */
                tmp_num_of_bmr = (*num_of_bmr) - 1;
                while (tmp_num_of_bmr >= 0) {
                    if ( (bm_run[tmp_num_of_bmr]->cmd_type == POPULATE)
                         || (bm_run[tmp_num_of_bmr]->cmd_type == POPULATE_INCREMENTALLY)
                         || (bm_run[tmp_num_of_bmr]->cmd_type == POPULATE_CONDITIONALLY)) {
                        bm_run[*num_of_bmr]->subscribers =
                            bm_run[tmp_num_of_bmr]->subscribers;
                        break;
                    }
                    tmp_num_of_bmr--;
                }
                /* Extract the keywords from the command */
                extractStringKeyword(line, "name",
                                     bm_run[*num_of_bmr]->test_run_name, W_L);

                /* extract, then overwrite if defined in the cmd line */
                extractIntKeyword(line, "warm_up_duration",
                                  &(bm_run[*num_of_bmr]->warm_up_duration));
                extractIntKeyword(line, "run_duration",
                                  &(bm_run[*num_of_bmr]->run_duration));
                extractIntKeyword(line, "repeats",
                                  &(bm_run[*num_of_bmr]->repeats));
                extractStringKeyword(line, "transaction_mix",
                                     bm_run[*num_of_bmr]->transaction_mix_str,
                                     W_L);
                extractStringKeyword(line, "database_client_distribution",
                                     bm_run[*num_of_bmr]->
                                     client_distribution_str, W_L);

                if (tdf_cmdline != NULL) {
                    if (tdf_cmdline->repeats != UNDEFINED_VALUE) {
                        bm_run[*num_of_bmr]->repeats = tdf_cmdline->repeats;
                    }
                    if (tdf_cmdline->run_duration != UNDEFINED_VALUE) {
                        bm_run[*num_of_bmr]->run_duration = tdf_cmdline->run_duration;
                    }
                    if (tdf_cmdline->warm_up_duration != UNDEFINED_VALUE) {
                        bm_run[*num_of_bmr]->warm_up_duration = tdf_cmdline->warm_up_duration;
                    }
                    if (tdf_cmdline->subscribers != UNDEFINED_VALUE) {
                         bm_run[*num_of_bmr]->subscribers = tdf_cmdline->subscribers;
                    }
                    if (*(tdf_cmdline->tr_mixes[0].name) != '\0') {
                        strncpy(bm_run[*num_of_bmr]->transaction_mix_str, tdf_cmdline->tr_mixes[0].name, W);
                    }
                    if (*(tdf_cmdline->client_distributions[0].name) != '\0') {
                        strncpy(bm_run[*num_of_bmr]->client_distribution_str,
                                tdf_cmdline->client_distributions[0].name, W);
                    }
                }

                if (!isEmptyBuf(line, strlen(line))) {
                    sprintf(msg, "Unknown content (%s) in run benchmark "
                            "command in TDF.",
                            line);
                    message('E', msg);
                    return E_ERROR;
                }
                break;
            case EXECUTESQL:
            case EXECUTESQLFILE:
		        bm_run[*num_of_bmr]->cmd_type = EXECUTESQLFILE;
		        if (extractStringKeyword(line,
                                         "file",
                                         bm_run[*num_of_bmr]->sql_file,
                                         W_L)
                    == E_NO_KEYWORD) {
                    extractStringKeyword(line, "sql",
                                         bm_run[*num_of_bmr]->sql_file, W_L);
                    bm_run[*num_of_bmr]->cmd_type = EXECUTESQL;
                }
		        break;
            case SLEEP:
                bm_run[*num_of_bmr]->cmd_type = SLEEP;
                extractIntKeyword(line, "duration",
                                  &(bm_run[*num_of_bmr]->run_duration));
                break;
            case NOP:
                message('E', "Unknown operation type in TDF [Test sequence] "
                        "section");
                /* Not fatal, because we can handle the next TDF */
                return E_ERROR;
        }
        /* update repeat counter */
        if (*repeats == 0) {
            *repeats = bm_run[*num_of_bmr]->repeats - 1;
        } else {
            (*repeats)--;
        }

        (*num_of_bmr)++;

        return 0;
}

/*##**********************************************************************\
 *
 *      initBMRunParameters
 *
 * Initializes the parameter values for a benchmark run.
 *
 * Parameters:
 *      tdf
 *          the test definition data structure
 *
 *      bm_run
 *			benchmark run data structure
 *
 * Return value:
 *      none
 */
void initBMRunParameters(struct tdfs *tdf, struct bmr *bm_run)
{
        bm_run->subscribers = tdf->subscribers;
        bm_run->min_subscriber_id = 1;
        bm_run->serial_keys = tdf->serial_keys;
        bm_run->commitblock_size = tdf->commitblock_size;
        bm_run->post_population_delay = tdf->post_population_delay;
        bm_run->warm_up_duration = tdf->warm_up_duration;
        bm_run->run_duration = tdf->run_duration;
        bm_run->repeats = tdf->repeats;
        bm_run->test_run_name[0] = '\0';
}

/*##**********************************************************************\
 *
 *      digestBasicOperationType
 *
 * Checks if a character string given as an argument starts with a
 * basic operation type tag, that is, either 'populate' or 'run'.  As a
 * side effect, abovementioned reserved words are cleaned from the line
 * (filled with spaces)
 *
 * Parameters:
 *      line
 *          the character string
 *
 *      operationType
 *          the basic operation type
 *
 * Return value:
 *      0 - success (always)
 */
int digestBasicOperationType(char *line,
                             enum test_sequence_cmd_type *operationType) {
        char *c;
        if (line == strstr(line, "populate")) {
            /* Note. The above checks that the line starts with
               "populate " */
            for (c = line; (*c != ' ' && *c != '\0'); c++) {
                *c = ' ';
            }
            if ((c = strstr(line, "conditional")) != NULL) {
                *operationType = POPULATE_CONDITIONALLY;
                for ( ; (*c != ' ' && *c != '\0'); c++) {
                    *c = ' ';
                }                
            } else if ((c = strstr(line, "incremental")) != NULL) {
                *operationType = POPULATE_INCREMENTALLY;
                for ( ; (*c != ' ' && *c != '\0'); c++) {
                    *c = ' ';
                }
            } else {
                /* 'incremental' or 'conditional'
                   was not found after 'populate' */
                *operationType = POPULATE;
            }
        }
        else if (line == strstr(line, "run")) {
            /* Note. The above checks that the line starts with
               "run " */
            for (c = line; (*c != ' ' && *c != '\0'); c++) {
                *c = ' ';
            }
            *operationType = RUN;
        }
        else if (line == strstr(line, "execute")) {
	        for (c = line; (*c != ' ' && *c != '\0'); c++) {
                *c = ' ';
            }
            *operationType = EXECUTESQL;
        }
        else if (line == strstr(line, "sleep")) {
            for (c = line; (*c != ' ' && *c != '\0'); c++) {
                *c = ' ';
            }
            *operationType = SLEEP;
        }
        else {
            *operationType = NOP;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      finalizeTestInNetwork
 *
 * Finalizes the test by sending the MSG_CLEAN type message
 * to all remote controls and local clients.
 *
 * Parameters:
 *      tdf
 *          Test definition data structure
 *
 *      bmrs
 *          Runtime benchmark data structure
 *
 *      mainClients
 *			Number of local clients
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int finalizeTestInNetwork(struct tdfs *tdf, const struct bmr *bmrs,
						  int mainClientProcesses) {
        int j;
        for (j = 0; tdf->client_distributions[
                     bmrs->client_distribution_ind].rem_loads[j].
                         remControls_index; j++) {
			if (sendDataS(
                        remControls[tdf->client_distributions[
                                        bmrs->client_distribution_ind].
                                            rem_loads[j].remControls_index].sck,
                        MAIN_CONTROL_ID, MSG_CLEAN, NULL)) {
                return -1;
			}
            /* Close all remote control sockets at this point (don't need the
               connections any more)*/
            portable_closesocket(
                    &(remControls[tdf->client_distributions[
                                      bmrs->client_distribution_ind].
                                          rem_loads[j].remControls_index].sck));
        }
        if (mainClientProcesses == 0) {
            /* No local clients */
            return 0;
        }
        /* make sure that local client connections are closed */
        /* actually, this should have already been done earlier
           in startTest and interruptTest */
        disconnectClientConnections();

        /* clean local clients also */
        cleanUpClients(mainClientProcesses);
        return 0;
}

/*##**********************************************************************\
 *
 *      isRemoteDefined
 *
 * Checks if the remote was defined in the remoteNodes file. Returns
 * the index in the structure remControls of the remote in question.
 *
 * Parameters:
 *      remoteName
 *          The name of the remote given in the tdf 'test' or
 *			'test_mix' directive.
 *
 * Return value:
 *      0  - remote not defined
 *    >=1  - remotes index in the remControls structure
 */
int isRemoteDefined(const char* remoteName)
{
        int i;
        i = 1;
        while (remControls[i].defined) {
            if (strncmp(remoteName, remControls[i].name,
                        strlen(remControls[i].name)) == 0) {
                /* the 'remoteName' found from defined
                   remote machines, return the index */
                return i;
            }
            i++;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      checkRemoteConnections
 *
 * Checks that we have a connection established to
 * the remote controls used in the test. If not, required
 * connections are established.
 *
 * Parameters:
 *      tdf
 *		    The test defintion data structure.
 *
 *      bmrs
 *		    The structure that holds all the active test definitions
 *
 * Return value:
 *      0  - connections succeeded
 *     !0  - connecting to a remote failed
 */
int checkRemoteConnections(struct tdfs *tdf, const struct bmr *bmrs)
{
        char msg[W_L];
        int j, remControlsIndex;
        /* Loop the remotes involved in this test */
        for (j = 0; tdf->client_distributions[
                     bmrs->client_distribution_ind].rem_loads[j].
                         remControls_index; j++) {
            remControlsIndex = tdf->client_distributions[
                    bmrs->client_distribution_ind].rem_loads[j].
                        remControls_index;

			if (!remControls[remControlsIndex].sck) {
                /* Remote not connected yet -> Connect */
                remControls[remControlsIndex].sck =
                    createConnection(remControls[
                                             remControlsIndex].ip,
                                     remControls[remControlsIndex].port);
                if (remControls[remControlsIndex].sck) {
                    sprintf(msg, "Connected to '%s'",
                            remControls[remControlsIndex].name);
                    message('D', msg);
                }
                else {
                    sprintf(msg, "Could not connect to '%s'",
                            remControls[remControlsIndex].name);
                    message('F', msg);
                    return -1;
                }
            }
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      pingRemotes
 *
 * Checks that the remote controls used in one test are responding. A
 * PING message (MSG_PING) is sent to each remote that carries the id
 * of the remote in question and the ip address.  The remotes reply to
 * the PING message.
 *
 * Parameters:
 *      tdf
 *          The test defintion data structure
 *
 *      bmrs
 *          The structure that holds all the active test definitions
 *
 * Return value:
 *      0  - remotes responding
 *     !0  - at least one remote not responding
 */
int pingRemotes(struct tdfs *tdf, const struct bmr *bmrs)
{
        char msg[256];
        char hostName[W_L];
        char buf[MAX_MESSAGE_SIZE];
        int missingResponses, j, k, retval,
            senderID, messageType, loopCounter;
        struct message_dataS data;
        int remControlsIndex;

        if (!(tdf->client_distributions[
                      bmrs->client_distribution_ind].
                          rem_loads[0].remControls_index)) {
            /* Not a single remote defined */
            return 0;
        }
        gethostname(hostName, W_L);
        /* Loop the remotes involved in this test to send
           each the ping messages */
        for (j = 0; tdf->client_distributions[
                     bmrs->client_distribution_ind].
                         rem_loads[j].remControls_index; j++) {
            remControlsIndex =
                tdf->client_distributions[bmrs->client_distribution_ind].
                                          rem_loads[j].remControls_index;
            /* send address of the main control in the message for callback */
            strncpy(data.sdata.reg.ip, hostName, W_L);
            data.sdata.reg.data =
                remControls[remControlsIndex].remoteControlId;
            data.utime = time(NULL);
            data.sdata.reg.testID = bmrs->test_run_id;
            sendDataS(remControls[remControlsIndex].sck,
                      MAIN_CONTROL_ID, MSG_PING, &data);
            remControls[remControlsIndex].pingStatus = 0;
        }
        /* Receive the ping response from all the remotes */
        missingResponses = 1;
        message('D', "Waiting for ping responses from the remotes");
        loopCounter =
            MAX_CONTROL_RESPONSE_WAIT_TIME / MESSAGE_RESPONSE_LOOP_SLEEP_TIME;
        while (missingResponses && loopCounter > 0) {
            retval = receiveMessage(&g_comm, buf);
            if (retval > 0) {
                if (decodeMessage(buf, &senderID, &messageType, &data) != 0) {
                    message('E', "Internal error from "
                            "the communication module");
                    /* Try to continue */
                }
                if (messageType != MSG_PING) {
                    sprintf(msg, "Unexpected message (got:%d, expected:%d) "
                            "received from remote %d",
                            messageType, MSG_PING, senderID);
                    message('W', msg);
                    /* Try to continue */
                }
                else {
                    /* Lets not decrease the loop counter if we
                       actually got a PING */
                    loopCounter++;
                }
                /* The following loop is safe cause we are sure to find the
                   corresponding remote information from the structure
                   'remControls' */
                for (k = 0; remControls[k].remoteControlId != senderID; k++) ;
                remControls[k].pingStatus = 1;
                sprintf(msg, "Received ping response from the remote %d",
                        senderID);
                message('D', msg);
            }
            else if (retval < 0) {
                message('E', "Internal error from the communication module");
                /* Try to continue */
            }
            /* Check if we are still waiting for ping responses */
            missingResponses = 0;
            for (j = 0; tdf->client_distributions[
                         bmrs->client_distribution_ind].
                             rem_loads[j].remControls_index;
                 j++) {
                if (remControls[tdf->client_distributions[
                                    bmrs->client_distribution_ind].
                                        rem_loads[j].remControls_index].
                    pingStatus == 0) {
                    missingResponses = 1;
                    break;
                }
            }
            if (missingResponses == 0) {
                /* All remotes have responded to the ping request */
                return 0;
            }
            SLEEP(MESSAGE_RESPONSE_LOOP_SLEEP_TIME); /* time for a short nap */
            loopCounter--;
        }
        /* We did not get all the responses within reasonable time */
        message('E', "Not all the remotes responding to a 'ping' request");
        return -1;
}

/*##**********************************************************************\
 *
 *      getClientResponses
 *
 * Waits an OK message from all the remotes and local clients.
 *
 * Parameters:
 *      tdf
 *          Pointer to tdf struct that holds tdf information
 *
 *      bmrs
 *		    The structure that holds the active test definition
 *
 *      waitDatabaseStart
 *          value containing wait method for database start
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int getClientResponses(struct tdfs *tdf, struct bmr *bmrs,
                       int waitDatabaseStart)
{
        char msg[256];
        char buf[MAX_MESSAGE_SIZE];
        int missingResponses, j, k, retval,
            senderID, messageType, loopCounter, numOfLocalClients;
        struct message_dataS data;
        /* Status flag indicating if the client was succesfully started */
        short clientsUp[MAX_CONNECTIONS+1];

        if (bmrs->cmd_type == RUN) {
            for (j = 0; tdf->client_distributions[
                         bmrs->client_distribution_ind].rem_loads[j].
                         remControls_index; j++) {
                remControls[tdf->client_distributions[
                                bmrs->client_distribution_ind].rem_loads[j].
                                remControls_index].
                    clientsUp = 0;
            }
        }
        numOfLocalClients = tdf->client_distributions[bmrs->
                                                      client_distribution_ind].
                                                      localLoad;

        for (j = 1; j <= numOfLocalClients; j++) {
            /* Local client numbers start from 1 */
            clientsUp[j] = 0;
        }

        /* Receive the OK (or INTR) message from all the remotes
           and local clients */
        missingResponses = 1;
        message('D', "Waiting for OK messages from the remotes "
                "and local clients");
        loopCounter =
            MAX_CONTROL_RESPONSE_WAIT_TIME / MESSAGE_RESPONSE_LOOP_SLEEP_TIME;
        while (missingResponses && loopCounter > 0) {
            retval = receiveMessage(&g_comm, buf);
            if (retval > 0) {
                if (decodeMessage(buf, &senderID, &messageType, &data) != 0) {
                    message('E', "Internal error from the "
                            "communication module");
                    /* Try to continue */
                }
                if (messageType != MSG_OK) {
                    if (messageType == MSG_INTR) {
                        sprintf(msg, "Test interrupt request from sender ID:%d",
                                senderID);
                    }
                    else {
                        sprintf(msg, "Unexpected message (got:%d, expected:%d) received from sender ID:%d",
                                messageType, MSG_OK, senderID);
                    }
                    message('E', msg);
                    return -1;
                }
                else {
                    /* Lets not decrease the loop counter if we
                       actually got an OK */
                    loopCounter++;
                }
                if (senderID < MAIN_CONTROL_ID) {
                    /* Get a message from a remote control */
                    /* The following loop is safe cause we are sure to find the
                       corresponding remote information from the structure
                       'remControls' */
                    for (k = 0; remControls[k].remoteControlId != senderID;
                         k++) ;
                    remControls[k].clientsUp = 1;
                    sprintf(msg, "Received OK message from remote ID:%d",
                            senderID);
                    message('D', msg);
                }
                else {
                    /* Got a message from a local client */
                    sprintf(msg, "Received OK message from sender ID:%d",
                            senderID);
                    message('D', msg);
                    clientsUp[senderID] = 1;
                }
            }
            else if (retval < 0) {
                message('E', "Internal error from the communication module");
                /* Try to continue */
            }
            /* Check if we are still waiting for OK messages */
            missingResponses = 0;
            if (bmrs->cmd_type == RUN) {
                for (j = 0; tdf->client_distributions[
                             bmrs->client_distribution_ind].rem_loads[j].
                         remControls_index; j++) {
                    if (remControls[
                                tdf->client_distributions[bmrs->
                                                      client_distribution_ind].
                                                      rem_loads[j].
                                                      remControls_index].
                                                      clientsUp == 0) {
                        missingResponses = 1;
                        break;
                    }
                }
            }
            if (!missingResponses) {
                for (j = 1; j <= numOfLocalClients; j++) {
                    if (clientsUp[j] == 0) {
                        missingResponses = 1;
                        break;
                    }
                }
            }
            if (missingResponses == 0) {
                /* All remotes and local clients have sent an OK message  */
                return 0;
            }
            SLEEP(MESSAGE_RESPONSE_LOOP_SLEEP_TIME); /* time for a short nap */

            /* loop continuously (do not decrease counter)
               if negative wait time given */
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
 *      propagateTestTime
 *
 * Propagates the test time to all the remotes. The remotes propagate
 * the time to their clients. Waits the return message from all the
 * remotes and based on the return time counts the synch. delay of
 * each remote. If the delay is too long for a remote returns an
 * error.  Propagates the test time to local clients also and checks
 * that they are well synchronized.
 *
 * Parameters:
 *      testTimer
 *          test timer
 *
 *      clientSynchThreshold
 *			Client synchronization threshold value
 *
 *      tdf
 *			Test definition data structure
 *
 *      bmrs
 *			test info object
 *
 * Return value:
 *      0  - all the clients well synchronized
 *     !0  - at least one client out of synch.
 */
int propagateTestTime(struct timertype_t *testTimer, int clientSynchThreshold,
                      struct tdfs *tdf, struct bmr *bmrs)
{
        int j, retval, senderID, messageType, receivedTestTime;
        int totalRemoteClients, numOfLocalClients;
        __int64 currentTestTime, loopTime;
        struct message_dataS data;
        char txt_buf[256];
        char buf[MAX_MESSAGE_SIZE];
        struct timertype_t loopTimer;
        int remoteIndex;

        initTimer(&loopTimer, TIMER_MILLISECOND_SCALE);

        data.sdata.reg.testID = bmrs->test_run_id;
        /* Propagate the test time to all the remotes */
        if (bmrs->cmd_type == RUN) {
            for (j = 0; tdf->client_distributions[
                         bmrs->client_distribution_ind].rem_loads[j].
                     remControls_index; j++) {
                remoteIndex = tdf->client_distributions[
                        bmrs->client_distribution_ind].rem_loads[j].
                    remControls_index;
                readTimer(testTimer, &currentTestTime);
                /* Casting from __int64 to int should not be a problem
                   (the value in currentTestTime is small enough */
                data.sdata.reg.data = currentTestTime;
                data.utime = time(NULL);
                data.sdata.reg.ip[0] = '\0';
                data.sdata.reg.testID = 0;   /* not used with MSG_TIME */
                if (sendDataS(remControls[remoteIndex].sck, MAIN_CONTROL_ID,
                              MSG_TIME, &data) != 0) {
                    sprintf(txt_buf, "Error sending test time to the remote %d",
                            remControls[remoteIndex].remoteControlId);
                    message('E', txt_buf);
                    return -1;
                }
                loopTime = 0;
                startTimer(&loopTimer);
                while (loopTime < MAX_CONTROL_RESPONSE_WAIT_TIME) {
                    retval = receiveMessage(&g_comm, buf);
                    if (retval > 0) {
                        /* We got a message -> read the test time first */
                        readTimer(testTimer, &currentTestTime);
                        if (decodeMessage(buf, &senderID, &messageType,
                                          &data) != 0) {
                            message('E', "Internal error from the communication "
                                    "module");
                            return -1;
                        }
                        if (messageType != MSG_TIME) {
                            sprintf(txt_buf, "Unexpected message (got:%d, expected:%d) "
                                    "received from sender ID:%d",
                                    messageType, MSG_TIME, senderID);
                            message('W', txt_buf);
                            return -1;
                        }
                        if (senderID != remControls[remoteIndex].remoteControlId) {
                            sprintf(txt_buf, "Received message from unexpected "
                                    "sender ID:%d",
                                    senderID);
                            message('W', txt_buf);
                            return -1;
                        }
                        /* we got TIME message from the right sender. Compare the
                           time we got in the message to the current time
                           (the difference should be less than the client synch.
                           threshold */
                        receivedTestTime = data.sdata.reg.data;
                        /* Safe to cast __int64 to int (the value is small
                         * enough) */
                        if ((((int)currentTestTime - receivedTestTime) / 2)
                            > clientSynchThreshold) {
                            sprintf(txt_buf, "Clients of remote %d not well enough "
                                    "synchronized",
                                    senderID);
                            message('E', txt_buf);
                            sprintf(txt_buf, "Clients' test time difference up to "
                                    "%d milliseconds",
                                    ((int)currentTestTime - receivedTestTime) / 2);
                            message('E', txt_buf);
                            sprintf(txt_buf, "(Synchronization threshold was "
                                    "set to %d milliseconds)",
                                    clientSynchThreshold);
                            message('E', txt_buf);
                            return -1;
                        }
                        else {
                            sprintf(txt_buf, "Remote %d: clients test time "
                                    "difference at most %d ms",
                                    senderID,
                                    ((int)currentTestTime - receivedTestTime) / 2);
                            message('D', txt_buf);
                        }
                        break;
                    }
                    else if (retval < 0) {
                        message('E', "Internal error from the"
                                "communication module");
                        return -1;
                    }
                    readTimer(&loopTimer, &loopTime);
                }
                if (loopTime >= MAX_CONTROL_RESPONSE_WAIT_TIME) {
                    /* We did not get an answer from a remote in time */
                    sprintf(txt_buf, "Remote %d did not response to the TIME "
                            "message in %d ms",
                            remControls[remoteIndex].remoteControlId,
                            MAX_CONTROL_RESPONSE_WAIT_TIME);
                    message('E', txt_buf);
                    return -1;
                }
            }
            totalRemoteClients = 0;
            for (j = 0; tdf->client_distributions[bmrs->
                                                  client_distribution_ind].
                     rem_loads[j].remControls_index; j++) {
                totalRemoteClients += tdf->client_distributions[
                        bmrs->client_distribution_ind].
                    rem_loads[j].remLoad;
            }
        }

        numOfLocalClients = tdf->client_distributions[bmrs->
                                                      client_distribution_ind].
                                                      localLoad;
        if (numOfLocalClients > 0) {
            /* We are running local (main control) clients also */
            /* Connect to them at first */
            if (createClientConnections(numOfLocalClients, 1)) {
                /* Log error printed in the method */
                return -1;
            }
            if (testTimeToLocalClients(testTimer, clientSynchThreshold,
                                       numOfLocalClients)) {
                message('E', "Local (Main Control) clients not well enough "
                        "synchronized");
                return -1;
            }
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      testTimeToLocalClients
 *
 * Sends the test time to local (Main Control) clients. At the
 * same time checks that the clients are well synchronized.
 *
 * Parameters:
 *      testTimer
 *			test timer object
 *
 *      clientSynchThreshold
 *			maximum difference in the test time between different clients
 *
 *      numOfLocalClients
 *			number of local clients
 *
 * Return value:
 *     0  - clients well synchronized
 *    !0  - a client out of synch
 */
int testTimeToLocalClients(struct timertype_t *testTimer,
                           int clientSynchThreshold, int numOfLocalClients)
{
        int i, retval, senderID, messageType, synchMistake, maxSynchMistake;
        __int64 timeCheck, loopTime;
        struct message_dataS data;
        struct timertype_t loopTimer;
        char buf[MAX_MESSAGE_SIZE];
        char txt_buf[256];

        initTimer(&loopTimer, TIMER_MILLISECOND_SCALE);

        maxSynchMistake = 0;
        for (i = 0; i < numOfLocalClients; i++) {
            readTimer(testTimer, &timeCheck);
            /* Safe cast (the value of __int64 is small enough) */
            data.sdata.reg.data = timeCheck;
            data.utime = time(NULL);
            data.sdata.reg.ip[0] = '\0';  /* not used with MSG_TIME */
            data.sdata.reg.testID = 0;    /* not used with MSG_TIME */
            sendDataS(clientScks[i], MAIN_CONTROL_ID, MSG_TIME, &data);

            /* Receive the response from a client */
            loopTime = 0;
            startTimer(&loopTimer);
            while (loopTime < MAX_CLIENT_RESPONSE_WAIT_TIME) {
                retval = receiveMessage(&g_comm, buf);
                if (retval > 0) {
                    /* We got a message -> read the test time first */
                    readTimer(testTimer, &timeCheck);

                    if (decodeMessage(buf, &senderID,
                                      &messageType, &data) != 0) {
                        message('E', "Internal error from the communication "
                                "module");
                        return -1;
                    }
                    if (messageType != MSG_TIME) {
                        sprintf(txt_buf, "Unexpected message (got:%d, expected:%d) received from "
                                "sender ID:%d",
                                messageType, MSG_TIME, senderID);
                        message('W', txt_buf);
                        return -1;
                    }
                    /* we got TIME message */
                    /* Safe cast (the value of __int64 is small enough) */
                    synchMistake = (timeCheck - data.sdata.reg.data) / 2;
                    if (synchMistake > clientSynchThreshold) {
                        message('E', "Local clients not well enough "
                                "synchronized");
                        sprintf(txt_buf, "Client's test time difference up to "
                                "%d milliseconds",
                                synchMistake);
                        message('E', txt_buf);
                        sprintf(txt_buf, "(Synchronization threshold was"
                                "set to %d milliseconds)",
                                clientSynchThreshold);
                        message('E', txt_buf);
                        return -1;
                    }
                    sprintf(txt_buf, "Local client synch. error less "
                            "than %d ms", synchMistake+1);
                    message('D', txt_buf);
                    if (maxSynchMistake < synchMistake+1) {
                        maxSynchMistake = synchMistake+1;
                    }
                    break;
                }
                else if (retval < 0) {
                    message('E', "Internal error from "
                            "the communication module");
                    return -1;
                }
                readTimer(&loopTimer, &loopTime);
            }
            if (loopTime >= MAX_CLIENT_RESPONSE_WAIT_TIME) {
                /* We did not get an answer from a client in time */
                sprintf(txt_buf, "A client did not response to the TIME "
                        "message in %d ms",
                        MAX_CLIENT_RESPONSE_WAIT_TIME);
                message('E', txt_buf);
                return -1;
            }
        }
        sprintf(txt_buf, "Localhost: clients test time difference "
                "at most %d ms", maxSynchMistake);
        message('D', txt_buf);
        return 0;
}

/*##**********************************************************************\
 *
 *      startTest
 *
 * Sends the STARTTEST messages to all the remotes and
 * the local clients.
 *
 * Parameters:
 *      tdf
 *          test definition data structure
 *
 *      bmrs
 *		    active test info object
 *
 * Return value:
 *      0  - STARTTEST messages send succesfully
 *     !0  - error sending the messages
 */
int startTest(struct tdfs *tdf, struct bmr *bmrs)
{
        int j, numOfLocalClients, totalRemoteClients;
        char txt_buf[256];
        struct message_dataS data;
        int remoteIndex;
        /* Loop through the remotes and send the message
           to each of them */
        if (bmrs->cmd_type == RUN) {
            for (j = 0; tdf->client_distributions[
                         bmrs->client_distribution_ind].rem_loads[j].
                     remControls_index; j++) {
                remoteIndex =
                    tdf->client_distributions[bmrs->client_distribution_ind].
                    rem_loads[j].remControls_index;
                if (sendDataS(remControls[remoteIndex].sck, MAIN_CONTROL_ID,
                              MSG_STARTTEST, &data) != 0) {
                    sprintf(txt_buf, "Error sending STARTTEST to the remote %d",
                            remControls[remoteIndex].remoteControlId);
                    message('E', txt_buf);
                    sprintf(txt_buf, "Running the test without the remote %d",
                            remControls[remoteIndex].remoteControlId);
                    message('E', txt_buf);
                }
            }

            totalRemoteClients = 0;
            for (j = 0; tdf->client_distributions[
                         bmrs->client_distribution_ind].rem_loads[j].
                     remControls_index; j++) {
                totalRemoteClients +=
                    tdf->client_distributions[bmrs->client_distribution_ind].
                    rem_loads[j].remLoad;
            }
        }

        numOfLocalClients =
            tdf->client_distributions[bmrs->client_distribution_ind].localLoad;
        /* Loop through the local clients and send the message
           to each of them */
        for (j = 0; j < numOfLocalClients; j++) {
            if (sendDataS(clientScks[j], MAIN_CONTROL_ID,
                          MSG_STARTTEST, &data) != 0) {
                sprintf(txt_buf, "Error sending STARTTEST to a local client %d",
                        j+1);
                message('E', txt_buf);
                sprintf(txt_buf, "Running the test without the client %d", j+1);
                message('E', txt_buf);
            }
        }
        /* Close the connections to local clients (don't need the
           connections any more) */

        /* Client processes will exit before the control process */
        /* We disconnect the connections from the control side first
           to make sure that the client side will clean up nicely and
           release the port right away */

        disconnectClientConnections();
        return 0;
}

/*##**********************************************************************\
 *
 *      interruptTest
 *
 * Sends the INTR messages to all the remotes and
 * the local clients.
 *
 * Parameters:
 *      tdf
 *		    test defintion data structure
 *
 *      bmrs
 *		    active test info object
 *
 * Return value:
 *      0  - STARTTEST messages send successfully
 *     !0  - error sending the messages
 */
int interruptTest(struct tdfs *tdf, struct bmr *bmrs)
{
        int j, numOfLocalClients, totalRemoteClients;
        char txt_buf[256];
        struct message_dataS data;
        int remIndex;

        if (bmrs->cmd_type == RUN) {
            /* Loop through the remotes and send the message
               to each of them */
            for (j = 0; tdf->client_distributions[
                         bmrs->client_distribution_ind].rem_loads[j].
                     remControls_index; j++) {
                remIndex = tdf->client_distributions[
                        bmrs->client_distribution_ind].rem_loads[j].
                    remControls_index;
                if (remControls[remIndex].sck) {
                    if (sendDataS(remControls[remIndex].sck, MAIN_CONTROL_ID,
                                  MSG_INTR, &data) != 0) {
                        sprintf(txt_buf, "Error sending INTR to the remote %d",
                                remControls[remIndex].remoteControlId);
                        message('E', txt_buf);
                    }
                }
            }

            totalRemoteClients = 0;
            for (j = 0; tdf->client_distributions[
                         bmrs->client_distribution_ind].rem_loads[j].
                     remControls_index; j++) {
                totalRemoteClients +=
                    tdf->client_distributions[bmrs->client_distribution_ind].
                    rem_loads[j].remLoad;
            }
        }

        numOfLocalClients =
            tdf->client_distributions[bmrs->client_distribution_ind].localLoad;
        /* Loop through the local clients and send the message
           to each of them */
        for (j = 0; j < numOfLocalClients; j++) {
            if (clientScks[j]) {
                if (sendDataS(clientScks[j], MAIN_CONTROL_ID,
                              MSG_INTR, &data) != 0) {
                    sprintf(txt_buf, "Error sending INTR to a local client %d",
                            j+1);
                    message('E', txt_buf);
                }
            }
        }
        /* Close the connections to local clients (don't need the
           connections any more) */
        disconnectClientConnections();
        return 0;
}



/*##**********************************************************************\
 *
 *      archiveTestSessionLogs
 *
 * Parameters:
 *      tdf
 *		    test defintion data structure
 *
 */
void archiveTestSessionLogs(struct tdfs *tdf, int after_run)
{
        char        msg[300];        /* message buffer for writeLog() */
        char        path[W_L] = {'\0'};       /* for log file path */
        char        target[W_L];

        if (after_run == 0) {
            /* preparation */

            /* create directory structure for local log files */
            sprintf(path,
                    "%s/%04d%02d%02d_%d/%s/",
                    LOG_ARCHIVE_PATH,
                    tdf->start_date.year,
                    tdf->start_date.month,
                    tdf->start_date.day,
                    tdf->session_id, "localhost");

            sprintf(msg, "Creating directories in path '%s'.", path);
            message('D', msg);
            if (mkFullDirStructure(path)) {
                sprintf(msg,
                        "Directory structure '%s' could not be created "
                        "(already exists?)",
                        path);
                message('W', msg);
            }
        } else {
            /* collect logs after the run */

            /* path for local log files */
            sprintf(path,
                    "%s/%04d%02d%02d_%d/%s/",
                    LOG_ARCHIVE_PATH,
                    tdf->start_date.year,
                    tdf->start_date.month,
                    tdf->start_date.day,
                    tdf->session_id, "localhost");

            /* archive the main node test session log */
            strncpy(target, path, W_L);
            strncat(target, DEFAULT_LOG_FILE_NAME, W_L-strlen(target));
            copyFile(DEFAULT_LOG_FILE_NAME, target);

            /* archive statistics log file */
            strncpy(target, path, W_L);
            strncat(target, STATISTICS_LOG_FILE_NAME, W_L-strlen(target));

            if (copyFile(STATISTICS_LOG_FILE_NAME, target) == 0) {
                if (remove(STATISTICS_LOG_FILE_NAME) != 0){
                    sprintf(msg,
                            "Error deleting statistics log file '%s'",
                            STATISTICS_LOG_FILE_NAME);
                    message('E', msg);
                }
            }
        }
}


/*##**********************************************************************\
 *
 *      collectTestRunLogs
 *
 * Collects the test log files after each test run from the remote
 * nodes and local clients also
 *
 * Parameters:
 *      tdf
 *		    test definition data structure
 *
 *      bmrs
 *		    active test info object
 *
 * Return value:
 *      0  - success
 *     !0  - fatal error
 */
int collectTestRunLogs(char *workDirBase, struct tdfs *tdf, const struct bmr *bmrs)
{
        struct message_dataS data;
        int j;
        int loopCounter, senderID, messageType;
        int missingRemotes = 0;
        int retval = 0;
        char msg[W_L];
        char buf[MAX_MESSAGE_SIZE];
        char path[W_L];
        int remote_called[MAX_NUM_OF_REMOTE_COMPUTERS];
        int i = 0;
        int remIndex = 0;
        char target[W_L];
        char client_logname[W_L];

        char client_filename[W];
        int clientsPerProcess;
        int clientID;
        int n_remainingclients;

        /* loop through all benchmarks in test session */
        for (i = 0; i < MAX_NUM_OF_REMOTE_COMPUTERS; i++) {
            remote_called[i] = 0;
        }

        /* create directory hierarchy and send log request messages
           to remote nodes */
        for (j = 0; tdf->client_distributions[bmrs->client_distribution_ind].
                 rem_loads[j].remControls_index; j++) {
            if (bmrs->cmd_type == RUN) {
                remIndex = tdf->client_distributions[
                        bmrs->client_distribution_ind].rem_loads[j].
                            remControls_index;
                missingRemotes++;
                /* call each remote control only once */
                if (!(remote_called[remIndex])) {
                    /* compose a path */
                    sprintf(path, "%s/%04d%02d%02d_%d/%s/", LOG_ARCHIVE_PATH,
                            tdf->start_date.year, tdf->start_date.month,
                            tdf->start_date.day, tdf->session_id,
                            remControls[remIndex].name);
                    sprintf(msg, "Creating directories in path '%s'.", path);
                    message('D', msg);
                    /* create directory structure */
                    if (mkFullDirStructure(path)) {
                        sprintf(msg, "Directory structure '%s' could not be "
                                "created (already exists?)", path);
                        message('W', msg);
                    }
                    /* send log request to remote */
                    sendDataS(remControls[remIndex].sck, MAIN_CONTROL_ID,
                              MSG_LOGREQUEST, NULL);
                    remote_called[remIndex] = 1;

                    loopCounter =
                        MAX_CONTROL_RESPONSE_WAIT_TIME /
                        MESSAGE_RESPONSE_LOOP_SLEEP_TIME;

                    /* receive log files until MSG_OK is caught from remote */
                    while (loopCounter > 0) {
                        retval = receiveMessage(&g_comm, buf);
                        if (retval > 0) {
                            if (decodeMessage(buf, &senderID, &messageType,
                                              &data) != 0) {
                                message('E', "Internal error from the "
                                        "communication module");
                                /* Try to continue */
                            }
                            /* Lets not decrease the loop counter if we
                               actually got a message */
                            loopCounter++;

                            switch (messageType) {
                                case (MSG_OK):
                                    sprintf(msg, "Received OK message from "
                                            "the sender ID:%d", senderID);
                                    message('D', msg);
                                    missingRemotes--;
                                    break;
                                case (MSG_FILE):
                                    /* concatenate test_run_id (as a part of
                                       filename) into end of the path */
                                    sprintf(path, "%s/%04d%02d%02d_%d/%s/%d_",
                                            LOG_ARCHIVE_PATH,
                                            tdf->start_date.year,
                                            tdf->start_date.month,
                                            tdf->start_date.day,
                                            tdf->session_id,
                                            remControls[remIndex].name,
                                            bmrs->test_run_id);

                                    if (receiveFile(
                                                data.sdata.file.fileFragment,
                                                path)) {
                                        sprintf(msg,
                                                "Error receiving a file from "
                                                "the sender ID:%d.", senderID);
                                        message('E', msg);
                                    } else {
                                        sprintf(msg, "Received log file from "
                                                "the sender ID:%d.", senderID);
                                        message('D', msg);
                                    }
                                    break;
                                default:
                                    /* Decrease the loop counter if we actually
                                       got a message */
                                    loopCounter--;
                                    sprintf(msg, "Unexpected message (%d) "
                                            "received from sender ID:%d",
                                            messageType, senderID);
                                    message('W', msg);
                                    /* Try to continue */
                            }
                            if (messageType == MSG_OK) {
                                /* done with this remote */
                                break;
                            }
                        }
                        else if (retval < 0) {
                            message('E', "Internal error from the "
                                    "communication module");
                            /* Try to continue */
                        }
                        /* time for a short nap */
                        SLEEP(MESSAGE_RESPONSE_LOOP_SLEEP_TIME);
                        loopCounter--;
                    }
                }
            }
        }

        if (missingRemotes > 0) {
            /* We did not get all the responses within reasonable time */
            message('E', "Not all the remotes sent their logs.");
            message('E', "Collect the log files manually.");
        }

        sprintf(path, "%s/%04d%02d%02d_%d/%s/%d_", LOG_ARCHIVE_PATH,
                tdf->start_date.year, tdf->start_date.month,
                tdf->start_date.day, tdf->session_id, "localhost",
                bmrs->test_run_id);

        /* collect main node client logs */

        if (*workDirBase == '\0') {
            /* the same directory for everything */
            for (i = 1;i <= tdf->client_distributions[
                         bmrs->client_distribution_ind].localLoad; i++) {
                strncpy(target, path, W_L);
                sprintf(client_logname, CLIENT_LOGFILENAME_FORMAT, i);
                strncat(target, client_logname, W_L-strlen(target));
                
                copyFile(client_logname, target);
                if (remove(client_logname) != 0){
                    sprintf(msg, "Could not delete client log file '%s'",
                            client_logname);
                    message('W', msg);
                }
            }

        } else {
            /* (possibly multiple) separate working directories */
            clientsPerProcess = (int) (tdf->client_distributions[bmrs->client_distribution_ind].localLoad
                                       / tdf->client_distributions[bmrs->client_distribution_ind].localLoadProcesses);
            n_remainingclients = tdf->client_distributions[bmrs->client_distribution_ind].localLoad
                % tdf->client_distributions[bmrs->client_distribution_ind].localLoadProcesses;
            clientID = 1;
            
            for (i = 1;i <= tdf->client_distributions[bmrs->client_distribution_ind].localLoadProcesses;i++) {
                if (i == tdf->client_distributions[bmrs->client_distribution_ind].localLoadProcesses) {
                    clientsPerProcess += n_remainingclients;
            }
                for (j = 0; j < clientsPerProcess; j++) {
                    sprintf(client_logname, "%s%d/", workDirBase, i);
                    sprintf(client_filename, CLIENT_LOGFILENAME_FORMAT, clientID);
                    strncpy(target, path, W_L);
                    strncat(target, client_filename, W_L-strlen(target));
                    strncat(client_logname, client_filename, W_L-strlen(client_logname));
                    copyFile(client_logname, target);
                    if (remove(client_logname) != 0){
                        sprintf(msg, "Could not delete client log file '%s'",
                                client_logname);
                        message('W', msg);
                    }
                    clientID++;
                }
            }
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      cleanUpClients
 *
 * Clean up client processes after the test run.
 *
 * Parameters:
 *      clients
 *         number of client processes
 *
 * Return value:
 *      0 - success
 *
 * Limitations:
 *      not meaningful in Windows
 */
int cleanUpClients(int clientProcesses)
{
#ifndef WIN32
        int i, status;
        pid_t wpid;
        char msg[256];
        /* 'Wait' for client (child) process */
        /*  pid_t waitpid(pid_t pid, int *stat_loc, int options); */

        for (i = 0; i < clientProcesses; i++) {
            if (client_pid[i] > 0) {
                /* removes the finished child process from the system
                 * process table */
                wpid = waitpid(client_pid[i], &status, WUNTRACED | WNOHANG);
                if (WIFEXITED(status)) {
                    /* was exited normally */
                    sprintf(msg, "client pid %d exited, status=%d\n", client_pid[i],
                            WEXITSTATUS(status));
                } else if (WIFSIGNALED(status)) {
                    /* was exited with a signal */
                    sprintf(msg,"client pid %d killed (signal %d)\n", client_pid[i],
                            WTERMSIG(status));
                } else {
                    sprintf(msg, "Unexpected client pid %d status (0x%x)\n",
                            client_pid[i], status);
                    message('D', msg);
                }
            }
        }
        /* discard the client pid array */
        free(client_pid);
#endif
        return 0;
}

/*##**********************************************************************\
 *
 *      readConfigurationFile
 *
 * Check access to the configuration file and store the contents to
 * the ddf struct. Counts also the hash code of the content of the
 * file and stores that to the ddf structure
 *
 * Parameters:
 *      ddf
 *          Pointer to the data definition structure,
 *          which holds all the information related to
 *			the session.
 *
 * Return value:
 *     0  - success
 *    !0  - error
 */
int readConfigurationFile(struct ddfs *ddf)
{
        /* The target database configuration file handle */
        FILE *fp = NULL;
        size_t len = 0;
        /* The hash code variable */
        ULONG crc32 = 0xffffffff;
        unsigned char *bufPtr;

        if (openFile(&fp, ddf->configuration_file_name) != 0) {
            /* The target database configuration file could
               not be opened */
            return E_ERROR;
        }
        /* Check the length of the configuration file */
        len = fread(ddf->configuration_file_contents,
                    1, MAX_CONFIGURATION_FILE_LENGTH, fp);
        if (ferror(fp)) {
            /* reading failed */
            return E_ERROR;
        }
        if (!feof(fp)) {
            writeLog('W',
                     "Configuration file is too long. Benchmark is run anyway");
        }
        fclose(fp);
        if (len >= MAX_CONFIGURATION_FILE_LENGTH) {
            len = MAX_CONFIGURATION_FILE_LENGTH - 1;
            writeLog('W', "Target DB conf. file truncated (too long)");
        }
        ddf->configuration_file_contents[len] = '\0';

        /* The CRC32 checksum counting */
        bufPtr = ddf->configuration_file_contents;
        while (len--) {
            crc32 =
                (crc32 >> 8) ^ CRC32LookupTable[(crc32 & 0xFF) ^ *bufPtr++];
        }
        /* Exclusive OR the result with the beginning value */
        crc32 = crc32 ^ 0xffffffff;
        /* Convert the integer crc32 value into a char string */
        itoa(crc32, ddf->configuration_content_checksum, 16);

        return 0;
}

/*##**********************************************************************\
 *
 *      setDateTimeNow
 *
 * Sets the current date and time for the date and time structs.
 *
 * Parameters:
 *      d
 *          Pointer to the date struct
 *
 *      t
 *          Pointer to the time struct
 *
 * Return value:
 *      none
 */
void setDateTimeNow(DATE_STRUCT *d, TIME_STRUCT *t)
{
        time_t now;
        struct tm *time_tm;

        now = time(NULL);
        time_tm = localtime(&now);
        d->day = (SQLUSMALLINT)time_tm->tm_mday;
        d->month = (SQLUSMALLINT)time_tm->tm_mon + 1;
        d->year = (SQLSMALLINT)time_tm->tm_year + 1900;
        t->hour = (SQLUSMALLINT)time_tm->tm_hour;
        t->minute = (SQLUSMALLINT)time_tm->tm_min;
        t->second = (SQLUSMALLINT)time_tm->tm_sec;
}

/*#***********************************************************************\
 *
 *      parseFileHeader
 *
 * Tries to parse incoming message to look for file
 * name.
 *
 * Parameters:
 *      fileContent
 *          Source file header, possibly incomplete
 *
 *      target
 *          Pointer to the buffer for file name
 *
 *      targetLen
 *          Length of target, in characters
 *
 * Return value:
 *      Pointer to next character after the header if full header was parsed
 *      NULL - if more data needed
 */

static const char *parseFileHeader(const char *fileContent, char *target,
                                   int targetLen)
{
        const char *fileEnd = fileContent + strlen(fileContent);
        const char *p1, *p2, *p3;
        int length;

        /* Extract the file name (FILE_START_TAG is <TATP_INPUT_FILE>) */
        p1 = fileContent;
        while (p1 != fileEnd && *p1 != '>') {
            p1++;
        }
        if (p1 == fileEnd) {
            return NULL;
        }
        p1++;
        p2 = p1;
        while (p2 != fileEnd && *p2 != ',') {
            p2++;
        }
        if (p2 == fileEnd) {
            return NULL;
        }
        p3 = p2+1;
        /* Now the file name is between p1 and p2 */
        length = p2 - p1;
        if (length > targetLen - 1) {
            length = targetLen - 1;
        }
        memcpy(target, p1, length);
        target[length] = '\0';

        return p3;
}

/*##**********************************************************************\
 *
 *      receiveFile
 *
 * Receives a file content from the communication port.
 * Stores the file in the given path
 * The protocol of the received file is
 * FILE_START_TAGfileName,fileContentFILE_STOP_TAG
 *
 * Parameters:
 *      dataFragment
 *		    initial data fragment of the file message
 *
 *      path
 *          path to use for file
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int receiveFile(const char* dataFragment, const char* path)
{
        int retval;
        /* Sender ID of the received message */
        int senderID;
        /* Type of the received message. */
        int messageType;
        /* Struct to hold the received data */
        struct message_dataS data;
        char fileContent[MAX_MESSAGE_SIZE * 2];
        char msg[W_L];
        char target[W_L] = "";
        FILE *fp = NULL;
        char *finish = NULL;

        /* Append the filename to the path */
        if (path != NULL) {
            strncat(target, path, W_L);
        }

        /* Copy the first fragment of the file from the input parameter */
        strcpy(fileContent, dataFragment);
        for (;;) {

            const char *p = fileContent;
            if (fp == NULL) {
                p = parseFileHeader(fileContent,
                                    target + strlen(target),
                                    W_L - strlen(target));
                if (p != NULL) {
                    /* Open file */
                    fp = fopen(target, "w");
                    if (fp == NULL) {
                        sprintf(msg, "Could not open the file %s for writing.",
                                target);
                        writeLog('E', msg);
                        return E_ERROR;
                    }
                }
            }

            if (fp != NULL) {
                finish = strstr(p, FILE_STOP_TAG);
                if (finish != NULL) {
                    *finish = '\0';
                }
                /* Write the file content */
                if (fputs(p, fp) < 0) {
                    sprintf(msg, "Could not write a character to the file %s.",
                            target);
                    writeLog('E', msg);
                    return E_ERROR;
                }
                *fileContent = '\0';
            }

            if (finish != NULL) {
                break;
            }

            retval = receiveDataS(&g_comm, &senderID, &messageType, &data);
            if (retval == 0) {
                /* main and remote nodes can send files */
                if (!(senderID <= MAIN_CONTROL_ID)) {
                    sprintf(msg,
                            "Received a message from an unexpected sender '%d'",
                            senderID);
                    writeLog('E', msg);
                    return E_ERROR;
                }
                else {
                    if (messageType != MSG_FILE) {
                        writeLog('E', "Wrong message type received "
                                 "(MSG_FILE expected)");
                    }
                    else {
                        /* Append the message content in the file content */
                        strcat(fileContent, data.sdata.file.fileFragment);
                    }
                }
            }
            else {
                sprintf(msg,
                        "Error %d at receiveDataS() while waiting message.",
                        retval);
                writeLog('E', msg);
                return E_ERROR;
            }
        }
        if (fp != NULL) {
            fclose(fp);
        }
        return E_OK;
}

/*##**********************************************************************\
 *
 *      finalize
 *
 * End routines of the benchmark session.  Finalize the communication,
 * write the final report and close the log.
 *
 * Parameters:
 *      none
 * Return value:
 *      0  - success
 *     !0  - error
 */
int finalize( )
{
        char msg[W_L];
        int retval;

        if (tdf_cmdline != NULL) {
            free(tdf_cmdline);
        }
        if (ddf_cmdline != NULL) {
            free(ddf_cmdline);
        }
        retval = finalizeCommunication(&g_comm);
        if (retval) {
            writeLog('W',
                     "Could not finalize the communication system");
        }

        if (g_log.warningCount != 0) {
            sprintf(msg, "Control reported %ld warning messages",
                    g_log.warningCount);
            writeLog('I', msg);
        }
        if (g_log.errorCount == 0) {
            writeLog('I', "No errors");
        }
        else {
            if (controlModuleMode == MODE_MAIN_CONTROL) {
                sprintf(msg,
                        "Total of %ld errors in Main Control, "
                        "Statistics and Clients",
                        g_log.errorCount);
            }
            else {
                sprintf(msg,
                        "Total of %ld errors in Remote Control",
                        g_log.errorCount);
            }
            writeLog('I', msg);
        }

        writeLog('I', "*** End ***\n");

        finalizeLog();

#ifdef _DEBUG
        /* For TATP software performance analysis
           not needed in actual benchmark runs */
        saveMyTimings("CONTROL_TIMINGS.CSV");
#endif /* _DEBUG */

        return retval;
}
