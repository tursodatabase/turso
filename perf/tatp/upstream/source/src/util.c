/**********************************************************************\
**  source       * util.c
**  description  * Utility functions that are intended for common use
**                 in the TATP package
**
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifdef WIN32
#include <windows.h>   /* Windows header files */
#include <direct.h>
#else
#include <stdlib.h>    /* Unix header files */
#include <unistd.h>
#include <fcntl.h>
#endif

#include "util.h"
#include <time.h>      /* Common header files */
#include <string.h>
#include "const.h"
#include "timer.h"

#ifdef WIN32
#ifndef SOLID_BUILD
#include <sqltypes.h>
#include <odbcss.h>
#else /* SOLID_BUILD */       
#include <solidodbc3.h>
#endif /* SOLID_BUILD */
#else
#include <sys/stat.h>
#include <sys/types.h>
#endif

#include <stdlib.h> /* For testing purposes */

#ifdef _DEBUG
/* Cumulative times (tickers) */
__int64 timeForAnalysis[MAX_NUMBER_OF_TIMING_POINTS];
/* 0 means minus, 1 means plus */
int plusOrMinusTime[MAX_NUMBER_OF_TIMING_POINTS];
#endif

static char dbtype_identifiers[3][17] = {
  "Generic Database",
  "IBM solidDB",
  "Informix"
};

#define FINAL_LOG_BUF_SIZE 4096

/*##**********************************************************************\
 *
 *      mkFullDirStructure
 *
 * Creates directory structure of given path in the file system
 * (if not exist)
 *
 * Parameters:
 *      fullpath - Path to create
 *
 * Return value:
 *      0 - success
 *     !0 - error
 *
 */
int mkFullDirStructure (const char* fullpath) {
        char *dir_name;
        int i;
        unsigned int offset = 0;
        char msg[256];

        /* examine the given path string and create directory hierarchy
           level-by-level */
        do {
            i = strcspn(fullpath+offset,"/\\");
            if (i > 0) {
                dir_name = (char*) malloc(sizeof(char) * (offset+i+1));
                strncpy(dir_name, fullpath, offset+i);
                dir_name[offset+i] = '\0';
                offset += i + 1;
#ifdef WIN32
                if(_mkdir(dir_name)) {
#else
                if(mkdir(dir_name, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH )) {
#endif
                    if (errno != EEXIST) {
                        /* directory defined in LOG_ARCHIVE_PATH
                           probably missing ? */
                        sprintf(msg, "Cannot create a new directory "
                                "'%s'.", dir_name);
                        message('E', msg);
                        return -1;
                    }
                    /*  is ok */
                } else {
                    sprintf(msg, "Directory '%s' was successfully "
                            "created\n", dir_name );
                    message('D', msg);
                }
                free (dir_name);
            }
        } while ((offset < strlen(fullpath)) && (i > 0));

        /* all directories in given path successfully created or already
           existed */
        return 0;
}

/*##**********************************************************************\
 *
 *      error_c
 *
 * Checks and in case of an error reports the return value
 * for a connection handle.
 *
 * Parameters:
 *      hdbc - Connection handle
 *
 *      err - Return value
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int error_c (SQLHDBC hdbc, RETCODE err)
{
        char        szSqlState[SQL_SQLSTATE_SIZE + 1];
        char        szErrorMsg[256];
        SDWORD      naterr;
        SWORD       length;
        SQLRETURN   sqlret;
        log_t*      log;

        if (err == SQL_SUCCESS) {
            /* no error*/
            return 0;
        }

        log = getLogObject();

        if (err == SQL_SUCCESS_WITH_INFO && log->verbose < 5) {
            /* do not report */
            return 0;
        }
        /* get SQL error data */
        strcpy(szSqlState, "00000");
        strcpy(szErrorMsg, "unspecified error/no error/no diagnostic data");
        sqlret = SQLGetDiagRec(SQL_HANDLE_DBC, hdbc, 1,
                               CHAR2SQL(szSqlState), &naterr,
                               CHAR2SQL(szErrorMsg), sizeof(szErrorMsg),
                               &length);
        if ((sqlret != SQL_SUCCESS) && (sqlret != SQL_SUCCESS_WITH_INFO)) {
            /* The system call failed -> report and return */
            message('E', "SQLGetDiagRec failed");
            return E_ERROR;
        }

        if (err == SQL_SUCCESS_WITH_INFO) {
            /* report debug-level info */
            message('D', szSqlState);
            message('D', szErrorMsg);
            return 0;
        }
        /* report error */
        message('E', szSqlState);
        message('E', szErrorMsg);
        return E_ERROR;
}

/*##**********************************************************************\
 *
 *      error_s
 *
 * Checks and in case of an error reports the return value
 * for a statement handle.
 *
 * Parameters:
 *      hstmt
 *          Statement handle
 *
 *      err
 *          Return value
 *
 *      accepted_state
 *          non-NULL error state that should be accepted as OK
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int error_s (SQLHSTMT hstmt, RETCODE err, const char *accepted_state)
{
        char szSqlState[SQL_SQLSTATE_SIZE + 1];
        char szErrorMsg[256];
        SDWORD naterr = 0;
#ifdef _DEBUG
        char errtxt_buf[261];
#endif
        log_t* log;

        if (err == SQL_SUCCESS) {
            /* no error */
            return 0;
        }

        log = getLogObject();

        if (err == SQL_SUCCESS_WITH_INFO && log->verbose < 5) {
            /* do not report */
            return 0;
        }
        /* fill the error code fields. If get_error returns an error value,
           a system call failed -> return an error */
        if (get_error(hstmt, szSqlState, szErrorMsg, &naterr) != 0) {
            return E_ERROR;
        }

#ifdef _DEBUG
        strcpy(errtxt_buf, szSqlState);
        strcpy(errtxt_buf+strlen(szSqlState), " ");
        strcpy(errtxt_buf+strlen(szSqlState)+1, szErrorMsg);
#endif

        if (accepted_state != NULL && strcmp(accepted_state,
                                             (char*)szSqlState) == 0) {
            /* accepted error code found so return without any
               error reporting */
            return 0;
        }
        if (err == SQL_SUCCESS_WITH_INFO) {
            /* report debug-level info */
            message('D', szSqlState);
            message('D', szErrorMsg);
            return 0;
        }
        if (strcmp(szSqlState, "00000") == 0) {
            /* no error */
            return 0;
        } else {
            /* report error */
            message('E', szSqlState);
            message('E', szErrorMsg);
            return E_ERROR;
        }
}

/*##**********************************************************************\
 *
 *      get_error
 *
 * Get the SQL diagnostic record for a statement handle.
 *
 * Parameters :
 *      hstmt
 *          Connection handle
 *      szSqlState
 *          pointer to SQL state code buffer
 *      szErrorMsg
 *          pointer to SQL error message buffer
 *      naterr
 *          pointer to error code
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int get_error(SQLHSTMT hstmt, char *szSqlState, char *szErrorMsg,
			  SDWORD *naterr) {
        SWORD length;
        SQLRETURN sqlret;

        /* By default, the SqlState and ErrorMsg variables get
           "no error" values */
        strcpy(szSqlState, "00000");
        strcpy(szErrorMsg, "no error");

        sqlret = SQLGetDiagRec(SQL_HANDLE_STMT, hstmt,
                               1, CHAR2SQL(szSqlState),
                               naterr, CHAR2SQL(szErrorMsg),
                               256, &length);
        if ((sqlret != SQL_SUCCESS) && (sqlret != SQL_SUCCESS_WITH_INFO)) {
            /* The system call failed -> report and return */
            message('E', "SQLGetDiagRec failed (system call error)");
            return E_ERROR;
        }

        return 0;
}

/*##**********************************************************************\
 *
 *      initializeLog
 *
 * Initializes the globals of the logging system.
 *
 * Parameters:
 *      verbose
 *          Verbosity level
 *      module
 *	       Module name
 *      color
 *          Printing color
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int initializeLog(int verbose, const char *module, int color)
{
        log_t* log = getLogObject();

        log->moduleColor = color; /* Color for console print */
        log->fLog = NULL;         /* Log file */
        log->verbose = verbose;   /* Verbose level */
        log->warningCount = 0;    /* Count of warning messages */
        log->errorCount = 0;      /* Count of normal and fatal error
                                     messages */

        strncpy(log->moduleName, module, 14);

        return 0;
}

/*##**********************************************************************\
 *
 *      finalizeLog
 *
 * Closes the log file.
 *
 * Return value:
 *      0  - success
 *   != 0  - error
 */
int finalizeLog()
{
        log_t* log = getLogObject();
        if (log != NULL
            && log->fLog != NULL) {
            fclose(log->fLog);
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      createLog
 *
 * Creates the log file in append mode and sets the global fLog.
 *
 * Parameters:
 *      LogFileName
 *          Log file name.
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int createLog(const char *LogFileName)
{
        int f;
        log_t* log = getLogObject();

        if (*LogFileName == '\0') {
            /* Console only message */
            message('F', "No log file name given");
            return -1;
        }
        else {
            /* close previous log file if exist */
            finalizeLog();

            /* set global fLog file handle */
            log->fLog = fopen(LogFileName, "a");

            if (log->fLog == NULL) {
                /* fatal error */
                message('F', "Cannot open the log.");
                return -1;
            }

            /* do not inherit log file descriptor
               when starting child process(es) */
            if ((f = fileno(log->fLog)) != -1) {
                set_FD_cloexec_flag(f, 1);
            }

        }
        return 0;
}

/*##**********************************************************************\
 *
 *      set_FD_cloexec_flag
 *
 * Set or clear the FD_CLOEXEC flag for a file descriptor
 * so that a spawned child process will not inherit the
 * file descriptor.
 *
 * Parameters:
 *      file
 *          File handle
 *
 * Return value:
 *      0  - success
 *     !0  - error
 *
 * Limitations:
 *      Not meaningful in Windows
 */
int set_FD_cloexec_flag(int fd_no, int value)
{
#ifndef WIN32
        int fd_flags; /* descriptor flags */

        /* get current fd flags */
        fd_flags = fcntl (fd_no, F_GETFD, 0);
        if (fd_flags < 0) {
            return fd_flags;
        }

        /* set close-on-exec flag for not inheriting
           file descriptor in child process */
        if (value != 0) {
            fd_flags |= FD_CLOEXEC;
        } else {
            fd_flags &= ~FD_CLOEXEC;
        }

        /* apply the modified flag to descriptor */
        return fcntl (fd_no, F_SETFD, fd_flags);

#else
        /* fcntl is not implemented in Windows */
        return 0;
#endif
}

/*##**********************************************************************\
 *
 *      writeLog
 *
 * Writes a message to the console (always)
 * and to the log file. (if the log file is open).
 *
 * Parameters:
 *      type
 *          Message type (verbosity level)
 *      msg
 *          Message
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int writeLog(const char type, const char *msg)
{
        char str[20];
        time_t now;
        struct tm *time_tm;
        /* The size ought to enough for all the messages */
        char buffer[FINAL_LOG_BUF_SIZE];
        int backcolor = PRINT_COLOR_BLACK;
        const char* msg_ptr;

        log_t* log = getLogObject();

        if (log != NULL) {
            if (log->verbose == 0) {
                /* all logging disabled */
                return 0;
            }
            switch (type) {
                /* Based on the warning type (D,I,W,E and F) and the verbose
                   mode selected by the user (in the command line) we decide
                   whether we report the log record or not */
                case 'X':
                    if (log->verbose < 6) return 0;    /* 'D' == "extra debug" */
                    break;
                case 'D':
                    if (log->verbose < 5) return 0;    /* 'D' == "debug" */
                    break;
                case 'I':
                    if (log->verbose < 4) return 0;    /* 'I' == "informative" */
                    break;
                case 'W':
                    if (log->verbose < 3) return 0;    /* 'W' == "warnings" */
                    log->warningCount++;
                    break;
                case 'E':
                    if (log->verbose < 2) return 0;    /* 'E' == "errors" */
                    log->errorCount++;
                    backcolor = PRINT_COLOR_RED;
                    break;
                case 'F':
                    if (log->verbose < 1) return 0;    /* 'F' == "fatal" */
                    log->errorCount++;
                    backcolor = PRINT_COLOR_RED;
                    break;
                default:
                    /*      type = '?'; */
                    break;
            }

            /* compose a message from a timestamp and the message */
            now = time(NULL);
            time_tm = localtime(&now);
            /* standard date format */
            strftime(str, 20, STRF_TIMEFORMAT, time_tm);
            msg_ptr = msg;
            if (strlen(msg) > FINAL_LOG_BUF_SIZE-40) {
                /* The buffer size is not enough ... */
                msg_ptr = "CAN NOT DISPLAY A LOG MESSAGE <- BUFFER OVERFLOW";
            }
            sprintf(buffer,"%c %s %s %s\n",type, str, log->moduleName, msg_ptr);
            /* ... and print the message with appropriate color */
            colorprint(buffer, log->moduleColor, backcolor);

            if (log->fLog != NULL) {
                fputs(buffer, log->fLog);
                fflush(log->fLog);
                /* If the program crashes, there is the latest message
                   in the log */
            }
        } else {
            now = time(NULL);
            time_tm = localtime(&now);
            strftime(str, 20, STRF_TIMEFORMAT, time_tm);
            sprintf(buffer,"%c %s %s\n",type, str, msg);
            colorprint(buffer, PRINT_COLOR_YELLOW, PRINT_COLOR_RED);
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      message
 *
 * Handles messaging (printing text to the console and to
 * log file). This is a wrapper that calls the actual logging
 * function which performs the actual messaging.
 *
 * Parameters:
 *      type
 *          Message type (verbosity level)
 *      msg
 *          Message
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int message(const char type, char *msg)
{
	writeLog(type, msg);
	return 0;
}

/*##**********************************************************************\
 *
 *      colorprint
 *
 * Prints a text message to console, with the given
 * foreground and background colors.
 *
 * Parameters:
 *      buffer
 *          Message
 *      ForeColor
 *          foreground color
 *      BackColor
 *          background color
 *
 * Limitations:
 *      Works only in Windows systems.
 */
void colorprint(const char *buffer, int ForeColor, int BackColor)
{
#ifdef WIN32
        /* The color printing facility is only implemented for
           the Windows platform (for the other platforms the message
           is printed with default color) */
        HANDLE  hConsole;
        WORD    fc = (WORD) ForeColor;
        WORD    bc = (WORD) BackColor;
        WORD    wAttributesOld;
        CONSOLE_SCREEN_BUFFER_INFO csbi;

        if (fc == 0 && bc == 0) { /* Color not initialized */
            fc = 7;
        }

        /*  Open the current console input buffer. */
        if( ( hConsole = CreateFile(
                      "CONOUT$", GENERIC_WRITE | GENERIC_READ,
                      FILE_SHARE_READ | FILE_SHARE_WRITE,
                      0L, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, 0L) )
            == (HANDLE) -1 ) {
			/* Unable to open console so just print the log
               message traditional way and return
               (no error printing or other kind of error handling
               is needed) */
			printf("%s", buffer);
			return;
		}

		/*  Get and Save information on the console screen buffer. */
		if (GetConsoleScreenBufferInfo( hConsole, &csbi ) == 0) {
			/* The system call failed. This is not critical
			   so just print the
               log message traditional way and return */
			printf("%s", buffer);
			return;
		};
		wAttributesOld = csbi.wAttributes;

		/*  Write in color. */
		SetConsoleTextAttribute( hConsole, (WORD) ( (bc << 4) | fc) );
		printf("%s", buffer);

		/*  Restore the foreground and background color attribute. */
		SetConsoleTextAttribute( hConsole, wAttributesOld );
#else
	printf("%s", buffer);
#endif
}

/*##**********************************************************************\
 *
 *      CRC32Reflect
 *
 * Reflects CRC bits in the CRC32 lookup table. CRCTable has to be
 * defined as ULONG CRCTable[256].
 * Reflection is a requirement for the CRC-32 standard.
 *
 * Parameters:
 *      ref
 *          value to process
 *
 *      ch
 *          character
 *
 * Return value:
 *	   Bit-swapped value of ref
 */
ULONG CRC32Reflect(ULONG ref, char ch)
{
        ULONG ret_val = 0;
        int i;
        /*  Swap bit 0 for bit 7, bit 1 for bit 6 and so on ... */
        for (i = 1; i < (ch + 1); i++) {
            if (ref & 1) {
                ret_val |= 1 << (ch - i);
            }
            ref >>= 1;
        }
        return ret_val;
}

/*##**********************************************************************\
 *
 *      CRC32InitTable
 *
 * Builds the CRC Lookup table array
 *
 * Parameters :
 *      CRCtable
 *	       Pointer to the CRC32 lookup table. CRCTable defined
 *		   (in control.h) as ULONG CRCTable[256]
 */
void CRC32InitTable(ULONG *CRCTable)
{
        int i, j;
        /* Polynomial used by e.g., PKZip */
        ULONG ulPolynomial = 0x04c11db7;
        /* 256 values representing ASCII char codes */
        for (i = 0; i <= 0xFF; i++) {
            CRCTable[i] = CRC32Reflect(i, 8) << 24;
            for (j = 0; j < 8; j++) {
                CRCTable[i] = (CRCTable[i] << 1) ^
                    (CRCTable[i] & (1 << 31) ? ulPolynomial : 0);
                CRCTable[i] = CRC32Reflect(CRCTable[i], 32);
            }
        }
}

/*##**********************************************************************\
 *
 *      msSleep
 *
 * Sleep some milliseconds.
 *
 * Parameters :
 *      milliseconds
 *          Milliseconds to sleep.
 */
void msSleep(int milliseconds) {
#ifdef WIN32
        Sleep(milliseconds);
#else
        if (milliseconds % 1000 == 0) {
            /* in POSIX, sleep() takes seconds as parameter */
            sleep(milliseconds / 1000);
        }
        else {
            /* in POSIX, usleep() uses microseconds */
            usleep(milliseconds * 1000);
        }
#endif
}

/*##**********************************************************************\
 *
 *      getServerParameterValue
 *
 * Gets a parameter value from solidDB
 *
 * Parameters :
 *      testdb
 *          Target DB
 *
 *      parameter_name
 *          Parameter to query
 *
 *      value
 *          Pointer for storing the parameter value
 */
static RETCODE getServerParameterValue(SQLHDBC *testdb, char *parameter_name,
                                   char *value)
{
#ifdef SOLID_BUILD
        RETCODE err = 0;
        SQLHSTMT statement = 0;
        SQLCHAR szValue[128] = { '\0' };
        SQLINTEGER dummy = 0;
        SQLLEN outlen[2];
        char command[W];
        char *pos;
            
        err = SQLAllocHandle(SQL_HANDLE_STMT, *testdb, &statement);
        if (error_c(*testdb, err) != 0) {
            message('E', "SQLAllocHandle failed");
            return E_FATAL;
        }
        sprintf(command, "ADMIN COMMAND 'parameter %s'", parameter_name);
        err = SQLPrepare(statement, (SQLCHAR*)command, SQL_NTS);
        if (error_s(statement, err, NULL) != 0) {
            message('E', "SQLPrepare failed");
            return E_FATAL ;
        }
        if ( SQLBindCol(statement, 1, SQL_C_SLONG, &dummy, 0, &outlen[0])
             != SQL_SUCCESS ) {
            message('E', "SQLBindCol failed");
            return E_FATAL ;
        }
        if ( SQLBindCol(statement, 2, SQL_C_CHAR, szValue, 127, &outlen[1])
             != SQL_SUCCESS ) {
            message('E', "SQLBindCol failed");
            return E_FATAL ;
        }
        if ( SQLExecute(statement) != SQL_SUCCESS) {
            message('E', "SQLExecute failed");
            return E_FATAL ;
        }
        /* We only want the first row */
        if ( SQLFetch(statement ) == SQL_NO_DATA) {
            message('E', "ADMIN COMMAND 'parameter ' returned no data.");
            return E_FATAL ;
        }       
        err = SQLFreeStmt(statement, SQL_CLOSE);
        if ( error_s(statement, err, NULL) ) {
            writeLog('E', "SQLFreeHandle failed" );
        }
        
        strcpy(value, strchr(strchr((char*)szValue, ' ')+1, ' ')+1);
        pos = strchr(value, ' ');
        *pos = '\0';
        return err;
#else
        return SQL_ERROR;
#endif
    
}

/*##**********************************************************************\
 *
 *      detectTargetDB
 *
 * Detects the target database type by matching the name given by
 * SQL_DBMS_NAME query.
 *
 * Parameters:
 *      testdb
 *          Connection handle
 *
 *      db
 *          Pointer in which to store resolved DBMS type
 *
 *      version
 *          Pointer in which to store DBMS version
 *
 * Return value:
 *      SQL_SUCCESS - success
 *      other return codes - error
 */
RETCODE detectTargetDB (SQLHDBC* testdb, dbtype* db, char *version, int printvalues)
{
        RETCODE sqlerr;
        char dbmsBuf[1024];
        char msg[256];
        int i;
        char paramvalue[W] = {'\0'};
        
        sqlerr = SQLGetInfo(*testdb,
                            SQL_DBMS_NAME,
                            dbmsBuf,
                            sizeof(dbmsBuf), /* byte size, not character */
                            NULL);

        if (SQL_SUCCEEDED(sqlerr)) {
            if (printvalues) {
                sprintf(msg, "Target DBMS name: %s", dbmsBuf);
                message('I',msg);
            }
            *db = DB_GENERIC;
            for (i = 1; i < NUM_DB_TYPES; i++) {
                if (strcmp(dbmsBuf, dbtype_identifiers[i]) == 0) {
                    *db = i;
                    break;
                }
            }
            sqlerr = SQLGetInfo(*testdb,
                                SQL_DBMS_VER,
                                dbmsBuf,
                                sizeof(dbmsBuf), /* byte size, not character */
                                NULL);
            if (SQL_SUCCEEDED(sqlerr)) {
                if (printvalues) {
                    sprintf(msg, "Target DBMS version: %s", dbmsBuf);
                    message('I',msg);
                }
                strncpy(version, dbmsBuf, W);
            } else {
                return sqlerr;
            }

            if (*db == DB_SOLID) {
                sqlerr = getServerParameterValue(testdb, "srv.name",
                                                 paramvalue);
                if (printvalues) {
                    if (*paramvalue != '\0') {
                        sprintf(msg, "Target DBMS server name: '%s'", paramvalue);
                        message('I', msg);
                    }
                }
            }
            return SQL_SUCCESS;
        } else {
            return sqlerr;
        }
}

/*##**********************************************************************\
 *
 *      getTargetDBVersion
 *
 * Determine the target DB version 
 *
 * Parameters:
 *      ConnectString - database connection string
 *      pVersion - pointer to storage for the build ID
 *
 * Return value 
 *      0 for success, other for error
 */
int getTargetDBVersion(struct server_t **server, char *ConnectString, char *version)
{
        RETCODE  err = 0;
        dbtype   db_type = DB_GENERIC;
        char     msg[256];
        SQLHENV  testdb_env = SQL_NULL_HENV;
        SQLHDBC  testdb = SQL_NULL_HDBC;

        /* start local server if needed */
        if (*server == NULL) {
            err = startServer(server);
            if (err != 0) {
                sprintf(msg, "Could not start database server (%s), error %d",
                        server_name,
                        err);
                message('F', msg);
                return E_FATAL;
            }
        }
        /* Connect to the target database */
        if (ConnectDB(&testdb_env, &testdb, ConnectString,
                      "target database") ) {
            message( 'F', "ConnectDB failed" ) ;
            return E_FATAL ;
        }
        
        if (detectTargetDB (&testdb, &db_type, version,0) != SQL_SUCCESS) {
            message('E', "detectTargetDB failed");
            return E_FATAL;
        }

        if (testdb != SQL_NULL_HDBC) {
            DisconnectDB(&testdb_env, &testdb, "target database");
        }        
        return err;
}

/*##**********************************************************************\
 *
 *	ConnectDB
 *
 *	Connects to the defined database. The connection is tried for
 * 	DB_CONNECTION_RETRIES times.
 *
 * 	Parameters :
 *       env
 *           SQL environment handle
 *    	 dbc
 *           Database connection handle
 *       db_connect
 *           Database connection string
 *    	 db_name
 *           Database name (used in messages)
 *
 * 	Return value:
 *       0  - success
 *      !0  - error
 */
int ConnectDB(SQLHENV* env, SQLHDBC* dbc, char* db_connect, char* db_name)
{
        RETCODE     rc;
        int         in_length;
        char        in_string[256];
        char        out_string[1024];
        SQLSMALLINT out_length;
        char        msg[256];
        int         retrycount;

        *env = SQL_NULL_HENV;
        *dbc = SQL_NULL_HDBC;

        rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, env);
        if (rc != SQL_SUCCESS) {
            sprintf(msg, "Cannot allocate environment for %s.", db_name);
            message('E', msg);
            return E_ERROR;
        }

#ifdef WIN32
        rc = SQLSetEnvAttr(*env,
                           SQL_ATTR_ODBC_VERSION,
                           (SQLPOINTER)SQL_OV_ODBC3,
                           SQL_IS_INTEGER);

        if (rc != SQL_SUCCESS) {
            sprintf(msg, "Cannot set ODBC 3.0 for %s.", db_name);
            message('W', msg);
        }
#else
        rc = SQLSetEnvAttr(*env,
                           SQL_ATTR_ODBC_VERSION,
                           (SQLPOINTER)SQL_OV_ODBC2,
                           SQL_IS_INTEGER);
        if (rc != SQL_SUCCESS) {
            sprintf(msg, "Cannot set ODBC 2 for %s.", db_name);
            message('W', msg);
        }
#endif
        rc = SQLAllocHandle(SQL_HANDLE_DBC, *env, dbc);
        if (rc != SQL_SUCCESS) {
            sprintf(msg, "Cannot allocate connection for %s.", db_name);
            message('E', msg);
            return E_ERROR;
        }
        /* Copy the db_connect string to in_string*/
        strncpy(in_string, db_connect, 256);
        in_length = strlen(in_string);

        retrycount = DB_CONNECTION_RETRIES;
        rc = 1;
        while (rc != 0 && retrycount-- >= 0) {
            /* Try to connect for retrycount number of times */
            if (strstr(in_string, "DSN=") != NULL) {
                rc = SQLDriverConnect(*dbc,
                                      0,
                                      CHAR2SQL(in_string),
                                      in_length,
                                      CHAR2SQL(out_string),
                                      sizeof(out_string)-1,
                                      &out_length,
                                      SQL_DRIVER_NOPROMPT);
            } else {
                rc = SQLConnect(*dbc, CHAR2SQL(in_string), in_length,
                                CHAR2SQL(DEFAULT_DBUSER_UID), SQL_NTS,
                                CHAR2SQL(DEFAULT_DBUSER_PWD), SQL_NTS);
            }
            rc = (RETCODE) error_c(*dbc, rc);
            if (rc != 0) {
                sprintf(msg, "Cannot connect to %s.", db_name);
                message('W', msg);
                msSleep(100);
            }
        }
        if (rc != 0) {
            sprintf(msg,
                    "Cannot connect to %s after %d retries.",
                    db_name,
                    DB_CONNECTION_RETRIES);
            message('E', msg);
            return E_ERROR;
        }
        sprintf(msg, "Connected to %s.", db_name);
        message('D', msg);

        rc = SQLSetConnectAttr(*dbc,
                               SQL_ATTR_AUTOCOMMIT,
                               (SQLPOINTER) SQL_AUTOCOMMIT_ON,
                               0);
        if (rc != SQL_SUCCESS) {
            sprintf(msg, "Cannot set autocommit mode on for %s.", db_name);
            message('W', msg);
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      DisconnectDB
 *
 * Disconnects the database and releases environment and database handles
 *
 * Parameters :
 *      env
 *          SQL environment handle
 *      dbc
 *          Database connection handle
 *      db_name
 *          Database name (used in messages)
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int DisconnectDB(SQLHENV *env, SQLHDBC *dbc, char *db_name)
{
        RETCODE rc;
        char msg[256];

        if (dbc != NULL) {
            rc = SQLDisconnect(*dbc);
            if (error_c(*dbc, rc)) {
                sprintf(msg, "Cannot disconnect from %s.", db_name);
                message('W', msg);
            }
            rc = SQLFreeHandle(SQL_HANDLE_DBC, *dbc);
            error_c(*dbc, rc);
        }
        if (env != NULL) {
            rc = SQLFreeHandle(SQL_HANDLE_ENV, *env);
            error_c(*env, rc);
        }
        sprintf(msg, "Disconnected from %s.", db_name);
        message('D', msg);
        return 0;
}




/*##**********************************************************************\
 *
 *      processSQL
 *
 *
 * Processes the given SQL clause.
 *
 *
 */
int processSQL(char *sql, SQLHDBC *targetdb,
               struct server_t **server, char *connect_string)
{
        char msg[W_EL];
        SQLHENV testdb_env;
        SQLHDBC testdb;
        SQLHSTMT statement;

        char *cmd = NULL;
        int stat, err;

        cmd = (char*)malloc(sizeof(char));
        cmd[0] = '\0';

        if (targetdb != NULL) {
            testdb = *targetdb;
        } else {
            /* start local server if needed */
            if (*server == NULL) {
                err = startServer(server);
                if (err != 0) {
                    sprintf(msg, "Could not start database server (%s), error %d",
                            server_name,
                            err);
                    message('F', msg);
                    return E_FATAL;
                }
            }
            /* Connect to the target database */
            if (ConnectDB(&testdb_env, &testdb, connect_string,
                          "target database")) {
                message('F', "ConnectDB failed");
                return E_FATAL;
            }
        }
        
        stat = composeSQLCommand(sql, &cmd);

        if (stat == 0) {
            sprintf(msg, "SQL statement \"%s\" was not completed - ';' character was missing at the end.", sql);
            message('E', msg);
        } else if (stat == 1) {
            sprintf(msg, "Executing SQL: \"%s\"", cmd);
            message('I', msg);

            err = SQLAllocHandle(SQL_HANDLE_STMT, testdb, &statement);
            if (error_c(testdb, err) != 0) {
                message('E', "SQLAllocHandle failed");
                err = E_FATAL;
            }
            if (!err) {
                /* Execute the SQL command with SQLExecDirect */
                err = SQLExecDirect(statement, CHAR2SQL(cmd), SQL_NTS);
                if (error_s(statement, err, NULL) != 0) {
                    message('E', "SQLExecute failed");
                    message('D', cmd);
                    /* not a fatal error -> continue execution */;
                    err = 0;
                }
            }
            if (!err) {
                err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
                if (error_s(statement, err, NULL)) {
                    message('E', "SQLFreeHandle failed");
                    /* not a fatal error -> continue execution */;
                    err = 0;
                }
            }
            if (!err) {
                err = SQLEndTran(SQL_HANDLE_DBC, testdb, SQL_COMMIT);
                if (error_c(testdb, err)) {
                    message('E', "SQLEndTran failed");
                    /* not a fatal error -> continue execution */;
                    err = 0;
                }
            }
        }
        /* Disconnect from the target database if the connection was made
           in this function */       
        if (targetdb == NULL) {
            DisconnectDB(&testdb_env, &testdb, "target database");
        }

        free(cmd);
        return 0;
}

/*##**********************************************************************\
 *
 *      processSQLFile
 *
 * Processes the given SQL file. The SQL commands defined in the file
 * are executed against the target database.  In case of an error
 * returned by the database, the processing is stopped and the method
 * returns an error.
 *
 * If given the targetdb connection handle, that handle is used. Otherwise,
 * a new connection will be opened before executing the contents of the
 * SQL file and closed after that.
 *
 * If given a server handle, that server will be started
 *
 * Parameters:
 *      sqlFileName
 *          name of the SQL file
 *      targetdb
 *          Database connection handle or NULL if the connection is not
 *          open
 *      server
 *          pointer to local server or NULL if not used
 *
 * Return value:
 *      0  - success
 *      E_ERROR or E_FATAL - error
 */
int processSQLFile(char *sqlFileName, SQLHDBC *targetdb,
		   struct server_t **server, char *connect_string)
{
        char msg[W_EL];
        char line[W_EL];     /* read buffer */
        FILE *fSQLFile;
        RETCODE err;
        SQLHENV testdb_env;
        SQLHDBC testdb;
        SQLHSTMT statement;
        /* The database (SQL) command buffer */
        char *cmd = NULL;
        int stat, firstLineWithinACommand;

        testdb_env = SQL_NULL_HENV;
        testdb = SQL_NULL_HDBC;

        err = 0;
        firstLineWithinACommand = 1;
        sprintf(msg, "Processing SQL file '%s'",
                sqlFileName);
        message('I', msg);

        if (openFile(&fSQLFile, sqlFileName) != 0) {
            /* This should not actually happen, because the file
               was checked earlier */
            sprintf(msg, "Cannot open '%s'", sqlFileName);
            message('F', msg);
            return E_FATAL; /* Fatal error */
        }


        if (targetdb != NULL) {
            testdb = *targetdb;
        } else {
            /* start local server if needed */
            if (*server == NULL) {
                err = startServer(server);
                if (err != 0) {
                    sprintf(msg, "Could not start database server (%s), error %d",
                            server_name,
                            err);
                    message('F', msg);
                    fclose(fSQLFile);
                    return E_FATAL;
                }
            }
            /* Connect to the target database */
            if (ConnectDB(&testdb_env, &testdb, connect_string,
                          "target database")) {
                message('F', "ConnectDB failed");
                fclose(fSQLFile);
                return E_FATAL;
            }
        }

        /* Parse the file */
        while (readFileLine(fSQLFile, line, W_EL) != -1) {
            if (firstLineWithinACommand == 1) {
                /* Lets reserve an intial (small) buffer for the command.
                   More space is reserved as needed... */
                cmd = (char*)malloc(sizeof(char));
                if (cmd == NULL) {
                    message('F', "Dynamic memory allocation failed ..");
                    return E_FATAL;
                }
                cmd[0] = '\0';
                firstLineWithinACommand = 0;
            }
            stat = composeSQLCommand(line, &cmd);
            /* comment lines will be removed and stat is set to 0
               lines containing a SQL command will set stat to 1 */
            if (stat == E_FATAL) {
                if (targetdb == NULL) {
                    /* an error occurred, disconnect from the target database */
                    DisconnectDB(&testdb_env, &testdb, "target database");
                }
                fclose(fSQLFile);
                free(cmd);
                return E_FATAL;
            }
            else if (stat == 1) {
                /* A full SQL command read (=until a semicolon)
                   -> process it */
                sprintf(msg, "Executing command %s", cmd);
                message('D', msg);

                err = SQLAllocHandle(SQL_HANDLE_STMT, testdb, &statement);
                if (error_c(testdb, err) != 0) {
                    message('E', "SQLAllocHandle failed");
                    err = E_FATAL;
                }
                if (!err) {
                    /* Execute the SQL command with SQLExecDirect */
                    err = SQLExecDirect(statement, CHAR2SQL(cmd), SQL_NTS);
                    if (error_s(statement, err, NULL) != 0) {
                        message('E', "SQLExecute failed");
                        message('D', cmd);
                        /* not a fatal error -> continue execution */;
                        err = 0;
                    }
                }
                if (!err) {
                    err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
                    if (error_s(statement, err, NULL)) {
                        message('E', "SQLFreeHandle failed");
                        /* not a fatal error -> continue execution */;
                        err = 0;
                    }
                }
                if (!err) {
                    err = SQLEndTran(SQL_HANDLE_DBC, testdb, SQL_COMMIT);
                    if (error_c(testdb, err)) {
                        message('E', "SQLEndTran failed");
                        /* not a fatal error -> continue execution */;
                        err = 0;
                    }
                }
                free(cmd);
                firstLineWithinACommand = 1;
                if (err == E_FATAL) {
                    /* fatal error, stop processing */
                    fclose(fSQLFile);
                    /* Disconnect from the target database */
                    if (targetdb == NULL) {
                        DisconnectDB(&testdb_env, &testdb, "target database");
                    }
                    return err;
                }
            }
        } /* while readFileLine */

        fclose(fSQLFile);

        /* Disconnect from the target database if the connection was made
           in this function */       
        if (targetdb != NULL) {
            DisconnectDB(&testdb_env, &testdb, "target database");
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      minimum
 *
 * Returns smaller of the two integers.
 *
 * Parameters :
 *    a
 *          An integer
 *    b
 *          Another integer
 *
 * Return value:
 *    a     if a < b
 *    b     if a >= b
 */
int minimum (int a, int b)
{
        if (a < b) {
            return a;
        } else {
            return b;
        }
}

/*##**********************************************************************\
 *
 *      openFile
 *
 * Opens a file. Returns an error if fopen() fails.
 *
 * Parameters :
 *      fp
 *          Pointer to file handle
 *      filename
 *          Name of the file to be opened
 *
 * Return value:
 *      0        - success
 *      E_ERROR  - file not found
 */
int openFile(FILE **fp, char *filename)
{
        *fp = fopen(filename, "r");
        if (*fp == NULL) {
            return E_ERROR;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      openFileForWrite
 *
 * Opens a file for writing. Returns an error if fopen() fails.
 *
 * Parameters :
 *      fp
 *          Pointer to file handle
 *      filename
 *          Name of the file to be opened
 *
 * Return value:
 *      0        - success
 *      E_ERROR  - file not found
 */
int openFileForWrite(FILE **fp, char *filename)
{
        *fp = fopen(filename, "a");
        if (*fp == NULL) {
            return E_ERROR;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      createFileInSequence
 *
 * Creates and opens a file. If a file with that name already exists, it
 * will be moved to another name (next available filename_base.<number>)
 *
 * Returns an error if fopen() fails.
 *
 * Parameters :
 *
 *      fp
 *          Pointer to file handle
 *
 *      filename
 *          Name of the file to be opened
 *
 * Return value:
 *      0        - success
 *      E_ERROR  - file not found
 */
int createFileInSequence(FILE **fp, char *filename)
{
        moveFileInSequence(filename);

        /* create (or overwrite) a file */
        *fp = fopen(filename, "w");
        if (*fp == NULL) {
            return E_ERROR;
        }
        return 0;
}


/*##**********************************************************************\
 *
 *      moveFileInSequence
 *
 *      filename
 *          Name of the file to be tested and possibly moved
 *
 * Return value:
 *      0  - file moved
 *     >1  - file not found
 */
int moveFileInSequence (char *filename) {
        FILE* fFile;
        char targetname[W_L];
        int i, count;

        fFile = fopen(filename, "r");
        if (fFile == NULL) {
            return 1;
        } else {
            /* file with conflicting name was found, backup it */
            i = 0;
            do {
                i++;
                targetname[0] = '\0';
                fclose(fFile);
                /* TODO: add check that the filename contains a dot */
                /* or make sure this works if not */
                count =  strchr(filename, '.') + 1 - filename;
                strncat(targetname, filename, count);
                sprintf(targetname + strlen(targetname), "%d", i);
                strcat(targetname, filename + count - 1);
                fFile = fopen(targetname, "r");
            } while (fFile != NULL);

            /* copy filename to targetname */
            copyFile(filename, targetname);
            remove(filename);
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      openFileWithPath
 *
 * Appends given filename to given path before calling openFile().
 *
 * Parameters:
 *      fp
 *          Pointer to file handle
 *
 *      filename
 *          Name of the file to be opened
 *
 *      path
 *          Directory path where the file should be located
 *
 * Return value:
 *      0   - success
 *     !0   - error
 */
int openFileWithPath(FILE **fp, char *filename, char *path)
{
        char strTempFileName[W_L];
        strncpy(strTempFileName, path, W_L);
        strncat(strTempFileName, filename, W_L-strlen(strTempFileName)-1);
        return openFile(fp,strTempFileName);
}


/*##**********************************************************************\
 *
 *      readFileLine
 *
 * Reads one line from the file, handle given as an argument.
 * Removes the tail of the line (including LF and CR) to achieve OS
 * independency.
 *
 * Parameters :
 *      file
 *          File handle
 *      buf
 *          pointer to the character buffer for the line content
 *      bufLen
 *          length of the character buffer
 *
 * Return value:
 *     0  - success
 *	 !0  - end of file
 */
int readFileLine(FILE *file, char* buf, int bufLen)
{
        char *c;
        if (fgets(buf, bufLen, file) != NULL) {
            /* Remove the useless tail of the line (including LF and CR) */
            for (c = buf+strlen(buf)-1; c >= buf && *c > 0 && *c < ' '; c--) {
                *c = 0;
            }
            return 0;
        }
        else {
            /* NULL was returned by fgets, we interpret that as
               the end of file */
            return -1;
        }
}

/*##**********************************************************************\
 *
 *      removeComment
 *
 * Removes the comments (everything after the '//' tag or '--' tag)
 * from the given line
 *
 * Parameters :
 *      line
 *          pointer to the character buffer for the line content
 */
void removeComment(char* line)
{
        char *c;
        for (c = line; !((*c == '/' && *(c+1) == '/')
                         || (*c == '-' && *(c+1) == '-') || *c == '\0'); c++);
        *c = '\0';
}

/*##**********************************************************************\
 *
 *      removeExtraWhitespace
 *
 * Removes all tab characters and replaces two or more
 * consecutive spaces with one.
 * Overwrites the given string with a modified string.
 * Notice that the function preserves one space at the
 * beginning and at the end of the line.
 *
 * Parameters:
 *      string
 *          string to modify
 *      bufLen
 *          length of the string buffer
 *
 * Return value:
 *      >=0 - length of modified string
 *	    -1 - error error -> -1
 */
int removeExtraWhitespace (char *string, int bufLen)
{
        int length;
        int i;
        int j = 0;
        char *cleanstr;
        char c;

        length = strlen(string);
        cleanstr = (char*) calloc(length + 1, sizeof(char)); /* length + \0 */
        if (cleanstr == NULL) {
            /* memory allocation failed -> return -1 to indicate an error */
            return -1;
        }

        for (i = 0; i < length; i++) {
            c = string[i];
            if (c == '\t') {
                /* all tabs will be removed */
                continue;
            } else if ((c == ' ') && (j > 0)) {
                /* replace two or more consecutive spaces with one */
                if (cleanstr[j-1] != ' ') {
                    cleanstr[j] = c;
                    j++;
                }
            } else {
                /* write character */
                cleanstr[j] = c;
                j++;
            }
        }
        /* copy the string back */
        cleanstr[j] = '\0';
        strncpy(string, cleanstr, bufLen);
        free(cleanstr);
        /* return the length of the modified string */
        return strlen(string);
}

/*##**********************************************************************\
 *
 *      removeEscapeCharacters
 *
 * Removes all '\' characters from a string. Removes only
 * escape characters (followed by '/', '<' or '>').
 *
 * Overwrites the given string with the modified string.
 *
 * Parameters:
 *      string
 *          string to modify
 *      bufLen
 *          length of string buffer
 *
 * Return value:
 *      >=0 - length of the modified string
 *		-1 - error
 */
int removeEscapeCharacters (char *string, int bufLen)
{
        int length;
        int i;
        int j = 0;
        char *cleanstr;
        char c;
        int addChar = 0;

        length = strlen(string);
        cleanstr = (char*) calloc(length + 1, sizeof(char));  /* length + \0 */
        if (cleanstr == NULL) {
            /* memory allocation failed -> return -1 to indicate an error */
            return -1;
        }

        for (i = 0; i < length; i++) {
            c = string[i];
            addChar = 0;
            if (i < (length-1)) {
                if (c == '\\') {
                    if (strspn(&(string[i+1]), "/<>") == 0) {
                        /* next character is not an escaped character */
                        addChar = 1;
                    }
                } else {
                    /* not escaped */
                    addChar = 1;
                }
            } else {
                /* always add last character */
                addChar = 1;
            }
            if (addChar) {
                cleanstr[j] = c;
                j++;
            }
        }
        cleanstr[j] = '\0';
        /* copy the string back */
        strncpy(string, cleanstr, bufLen);
        free(cleanstr);
        /* return the length of the modified string */
        return strlen(string);
}

/*##**********************************************************************\
 *
 *      trim
 *
 * Removes all spaces from the start and the end of a string.
 * Overwrites the given string with the modified string.
 *
 * Parameters:
 *      string
 *          string to modify
 *
 * Return value:
 *      0 - success
 *     -1 - error
 *
 */
int trim (char* string)
{
        char c;
        int i;
        int length;
        char *temp;

        /* position of first non-space from beginning */
        i = strspn(string," ");
        length = strlen(string)-i;

        if (length > 0) {
            /* Allocate memory for temporal storage */
            temp = (char*)calloc(length + 1, sizeof(char));
            if (temp == NULL) {
                /* memory allocation failed -> return error */
                return -1;
            }
            /* copy the original to 'temp' string starting from first
               non-space character */
            strncpy(temp, string + i, length);
            temp[length] = '\0';

            i = length - 1;
            c = ' ';
            /* remove spaces from the end */
            for (i = length-1; (i>=0) && (c == ' '); i--) {
                c = temp[i];
                if (c == ' ') {
                    temp[i] = '\0';
                }
            }

            /* copy the string back to the original buffer */
            strncpy(string, temp, strlen(temp));
            string[strlen(temp)] = '\0';
            free(temp);
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      composeSQLCommand
 *
 * Composes an SQL command from the sub-strings. The return value indicates
 * if the command is completed (-> ';' was found). The line have to be NULL
 * terminated. Also the commandBuf has to be NULL terminated.
 *
 * Parameters :
 *      line
 *          Pointer to line containing the next segment
 *			to be concatenated in the command
 *      commandBuf
 *          A double pointer to the final command buffer
 *
 * Return value:
 *      1  - OK and the SQL command is completed
 *      0  - OK but the SQL command is not completed
 *      E_FATAL  - error
 */
int composeSQLCommand(char* line, char** commandBuf)
{
        int length;

        /* First clean up the line */
        removeComment(line);
        if (trim(line) == -1) {
            message('F', "Dynamic memory allocation failed");
            return E_FATAL;
        }
        length = removeExtraWhitespace(line, strlen(line));

        /* Empty line ? */
        if ((length == 0) || ((length == 1) && (line[0] == ' '))) {
            return 0;
        }

        /* Allocate space for the command */
        /* previous space + space char + new space + termination */
        length = strlen(*commandBuf);
        *commandBuf = (char *)realloc(*commandBuf,
                                      length + 1 + strlen(line)+1);
        if (*commandBuf == NULL) {
            message('F', "Dynamic memory allocation failed");
            return E_FATAL;
        }

        /* concatenate the line to existing cmd string */
        strncat(*commandBuf, line, strlen(line));
        strcat(*commandBuf, " ");

        if (line[strlen(line)-1] != ';') {
            /* the new line does not end with ';' so the SQL command
               is not completed */
            return 0;
        }
        else {
            /* The SQL command is ready (';' was met) */
            /* Remove the semicolon */
            (*commandBuf)[strlen(*commandBuf)-2] = '\0';
            if (strlen(*commandBuf) != 0) {
                return 1;
            }
            else {
                /* ';' was the only character */
                return 0;
            }
        }
}

/*##**********************************************************************\
 *
 *      isEmptyBuf
 *
 * Checks if a character buffer is empty (zero length or
 * filled with space characters).
 *
 * Parameters :
 *      buf
 *          the NULL terminated chacter buffer
 *      bufLen
 *			length of the char buffer
 *
 * Return value:
 *      1  - empty buffer
 *      0  - non-empty buffer
 */
int isEmptyBuf(const char *buf, size_t bufLen)
{
        size_t ind;
        for (ind = 0; ind < bufLen; ind++) {
            if (buf[ind] != ' ') return 0;
        }
        return 1;
}

/*##**********************************************************************\
 *
 *      simpleMatch
 *
 * One parameter match using regular expression.
 *
 * Parameters :
 *      r
 *          regex_t
 *      line
 *          String to be checked for match
 *      pattern
 *          Regular expression
 *      start
 *          Pointer to the start of the match in line
 *      length
 *          Pointer to the length of the match
 *
 * Return value:
 *     0  - match
 *     1  - match but without exactly one parameter
 *     2  - no match
 *    <0  - error
 */
int simpleMatch(regex_t *r, char *line, char *pattern,
				int *start, int *length)
{
        char errbuff[128];
        int error, n;
        regmatch_t *m;
        int retval;

        /* Initialize the data structure */
        memset(r, 0, sizeof(regex_t));
        /* Compile the regular expression first (ignore the case) */
        if ((error = regcomp(r, pattern, REG_EXTENDED | REG_ICASE))) {
            regerror(error, r, errbuff, sizeof(errbuff));
            writeLog('E', errbuff);
            regfree(r);
            return -1;
        }
        n = r->re_nsub + 1;
        m = (regmatch_t *) malloc(sizeof(regmatch_t) * n);
        if (m == NULL) {
            writeLog('E', "Cannot reserve memory for regmatch_t");
            regfree(r);
            return -1;
        }
        /* Perform the matching */
        error = regexec(r, line, n, m, 0);
        if (error == REG_NOMATCH) {
            /* No match found */
            free(m);
            regfree(r);
            return 2;
        }
        if (error) {
            regerror(error, r, errbuff, sizeof(errbuff));
            /* Actual error generated by the regexec method. Make a log entry */
            writeLog('E', errbuff);
            free(m);
            regfree(r);
            return -1;
        }
        /* Matching string was found. Determine the start position
           and the length of the match */
        retval = 1;
        if (n == 2 && m[0].rm_so >= 0 && m[1].rm_so >= 0) {
            *start = m[1].rm_so;
            *length = m[1].rm_eo - m[1].rm_so;
            retval = 0;
        }
        free(m);
        regfree(r);
        return retval;
}

/*##**********************************************************************\
 *
 *      multiMatch
 *
 * Multiple parameter match using regular expression.
 *
 * Parameters :
 *      r
 *          regex_t
 *      line
 *          String to be checked for match
 *      pattern
 *          Regular expression
 *      count
 *          Pointer to the count of parameters
 *      start
 *          Pointer to the start of the match in line
 *      length
 *          Pointer to the length of the match
 *
 * Return value:
 *
 *      0  - match
 *      2  - no match
 *      3  - at least one field not found
 *     <0  - error
 */
int multiMatch(regex_t *r, char *line, char *pattern,
			   int *count, int *start, int *length)
{
        char errbuff[128];
        int error, n, j;
        regmatch_t m[10];
        int retval;

        /* Initialize the data structure */
        memset(r, 0, sizeof(regex_t));
        /* Compile the regular expression first (ignore the case) */
        if ((error = regcomp(r, pattern, REG_EXTENDED | REG_ICASE))) {
            regerror(error, r, errbuff, sizeof(errbuff));
            writeLog('E', errbuff);
            regfree(r);
            return -1;
        }
        n = r->re_nsub + 1;
        /* Perform the matching */
        error = regexec(r, line, n, m, 0);
        if (error == REG_NOMATCH) {
            /* No match found */
            regfree(r);
            return 2;
        }
        if (error) {
            /* Actual error generated by the regexec method.
               Make a log entry */
            regerror(error, r, errbuff, sizeof(errbuff));
            writeLog('E', errbuff);
            regfree(r);
            return -1;
        }
        retval = 0;
        /* Matching string was found. Determine the start position
           and the length of the match */
        for (j = 0; j < n; j++) {
            if (m[j].rm_so < 0) {
                retval = 3; /* At least one field not found */
            }
            start[j] = m[j].rm_so;
            length[j] = m[j].rm_eo - m[j].rm_so;
        }
        *count = n;
        regfree(r);
        return retval;
}

/*##**********************************************************************\
 *
 *      fullMatch
 *
 * Matches a string. Looks for a full match, meaning that the regexp should
 * match as whole (match length equals string length).
 *
 * Discards results and uses only return value.
 *
 * Parameters :
 *      r
 *          regex_t
 *      line
 *          String to be checked for match
 *      pattern
 *          Regular expression
 *
 * Return value:
 *      0  - match
 *     <0  - error
 */
int fullMatch(regex_t *r, char *line, char *pattern)
{
        int match_count = 0;
        int starts[100];
        int lengths[100];
        int retval;
        int i;
        
        for (i = 0; i < 100 ; i++) {
            starts[i] = 0;
            lengths[i] = 0;
        }
        
        /* Do the matching */
        retval = multiMatch(r, line, pattern, &match_count, starts, lengths);

        if (retval < 0) {
            /* error */
            return retval;
        } else if ((starts[0] == 0) && lengths[0] == strlen(line)) {
            /* full match */
            return 0;
        } else {
            /* not match */
            return 1;
        }
}

/*##**********************************************************************\
 *
 *      fullMatch_e
 *
 * Matches a string. Looks for a full match, meaning that the regexp should
 * match as whole (match length equals string length).
 *
 * Parameters :
 *      r
 *          regex_t
 *      line
 *          String to be checked for match
 *      pattern
 *          Regular expression
 *
 * Return value:
 *      0  - match
 *     <0  - error
 */
int fullMatch_e(regex_t *r, char *line, char *pattern, int *count,
                int *starts, int *lengths)
{
        int retval;

        /* Do the matching */
        retval = multiMatch(r, line, pattern, count, starts, lengths);

        if (retval < 0) {
            /* error */
            return retval;
        } else if ((starts[0] == 0) && lengths[0] == strlen(line)) {
            /* full match */
            return 0;
        } else {
            /* not match */
            return 1;
        }
}

/*##**********************************************************************\
 *
 *      extractStringKeyword
 *
 * Checks if the given keyword is found from the given
 * character buffer. The value of the keyword in the buffer
 * has to be enclosed in quotes.
 * The syntax of a keyword/value is
 *    keyword = "value"
 * If found sets the value of the keyword to 'value'.
 * In the buffer 'buf' fills up the keyword and its value
 * with spaces.
 *
 * Parameters :
 *      buf
 *          Character buffer (has to be NULL terminated)
 *      keyword
 *          Keyword to be checked (has to be NULL termninated)
 *      value
 *          Pointer to the string value to be set.
 *      valueLen
 *          Length of the buffer 'value'
 *
 * Return value:
 *      0  - success, value set
 *      E_NO_KEYWORD - Keyword did not match
 *      !0  - error
 */
int extractStringKeyword(const char *buf, const char *keyword,
						 char *value, int valueLen)
{
        size_t KWlen;
        char *startKeyword, *c, *d, *e;
        char msg[256];

        if ((startKeyword = strstr(buf, keyword)) != NULL) {
            /* Keyword was found */
            KWlen = strlen(keyword);

            /* Find '=' */
            for (c = startKeyword + KWlen; !(*c == '=' || *c == '\0'); c++) ;
            if (*c != '=') {
                sprintf(msg, "No '=' after keyword '%s'.", keyword);
                message('E', msg);
                return E_ERROR;
            }
            /* Find the starting quotation mark */
            for (c++; *c == ' '; c++) ;
            if (*c != '"') {
                sprintf(msg, "No string after keyword '%s'.", keyword);
                message('E', msg);
                return E_ERROR;
            }
            c++;
            /* Find closing quotation mark */
            for (d = c; (*d != '"' && *d != '\0'); d++) ;
            if (*d != '"') {
                sprintf(msg, "No closing quotation mark for the keyword '%s'.",
                        keyword);
                message('E', msg);
                return E_ERROR;
            }
            *d = '\0';
            if ((d-c) < valueLen) {
                /* The value buffer has enough space to store the value */
                strcpy(value, c);
            }
            else {
                sprintf(msg, "Too long value for keyword '%s'.", keyword);
                message('E', msg);
                return E_ERROR;
            }

            /* fill up the keyword and its value with spaces */
            for (e = startKeyword; e <= d; e++) {
                *e = ' ';
            }
            return 0;
        }
        return E_NO_KEYWORD;   /* Keyword did not match */
}

/*##**********************************************************************\
 *
 *      extractIntKeyword
 *
 * Checks if the given keyword is found from the given
 * character buffer.
 * The syntax of a keyword/value is
 *    keyword = value
 * If found sets the value of the keyword to 'value'.
 * In the buffer 'buf' fills up the keyword and its value
 * with spaces.
 *
 * Parameters :
 *      buf
 *          Character buffer (has to be NULL terminated)
 *      keyword
 *          Keyword to be checked (has to be NULL terminated)
 *      value
 *          Pointer to the int value to be set.
 *
 * Return value:
 *     0  - success, value set
 *     E_NO_KEYWORD - Keyword did not match
 *     !0  - error
 */
int extractIntKeyword(const char *buf, const char *keyword, int *value)
{
        size_t KWlen;
        char *startKeyword, *c, *d, *e;
        char msg[256];

        if ((startKeyword = strstr(buf, keyword)) != NULL) {
            /* Keyword was found */
            KWlen = strlen(keyword);

            /* Find '=' */
            for (c = startKeyword + KWlen; !(*c == '=' || *c == '\0'); c++) ;
            if (*c != '=') {
                sprintf(msg, "No '=' after keyword '%s'.", keyword);
                message('E', msg);
                return E_ERROR;
            }
            c++;
            for ( ; *c == ' '; c++) ;
            /* find the end of the int value */
            for (d = c; (*d != ' ' && *d != '\0'); d++) {
                if (strcspn(d, "1234567890") != 0) {
                    /* not an integer value */
                    sprintf(msg, "No value of the right type for the keyword "
                            "'%s'.", keyword);
                    message('E', msg);
                    return E_ERROR;
                }
            }
            if (d == c) {
                /* no value for the keyword given */
                sprintf(msg, "No value was given for the keyword "
                        "'%s'.", keyword);
                message('E', msg);
                return E_ERROR;
            }

            *value = atoi(c);

            /* fill up the keyword and its value with spaces */
            for (e = startKeyword; e <= d; e++) {
                *e = ' ';
            }
            return 0;
        }
        return E_NO_KEYWORD;   /* Keyword did not match */
}

/*##**********************************************************************\
 *
 *      copyFile
 *
 * Copies a file from a given source path to the given target path.
 *
 * Parameters :
 *      sourceFile
 *          From where the data is copied.
 *      targetFile
 *          Where the data is put.
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int copyFile(char *sourceFile, char *targetFile)
{
        FILE* file_from;
        FILE* file_to;
        char msg[W_L];
        char buf[W_L+1];   /* input buffer */

        file_from = fopen(sourceFile, "r");
        if (!file_from) {
            sprintf(msg, "Cannot open source file: %s", sourceFile);
            message('E', msg);
            return -1;
        }
        file_to = fopen(targetFile, "w+");
        if (!file_to) {
            sprintf(msg, "Cannot open target file: %s", targetFile);
            message('E', msg);
            return -1;
        }

        while (!feof(file_from)) {
            if (fgets(buf, W_L+1, file_from) != NULL ) {
                if (fputs(buf, file_to) == EOF) {
                    sprintf(msg, "Error writing to target file: %s", targetFile);
                    message('E', msg);
                    return(-1);
                }
            }
        }

        /* use external line reading function to take care of different
           linebreaks */
        /* while (readFileLine(file_from, buf, W_L+1) == 0) {
            if (fputs(buf, file_to) == EOF) {
            sprintf(msg, "Error writing to target file: %s", targetFile);
            message('E', msg);
            return(-1);
            } else {
            fputs("\n", file_to);
            }
            }*/

        if (!feof(file_from)) {
            sprintf(msg, "Error reading from source file: %s", sourceFile);
            message('E', msg);
            return(-1);
        }

        /* close source and target file streams. */
        if (fclose(file_from) == EOF) {
            sprintf(msg, "Error when closing source file: %s", sourceFile);
            message('W', msg);

        }
        if (fclose(file_to) == EOF) {
            sprintf(msg, "Error when closing target file: %s", targetFile);
            message('W', msg);
        }
        return 0;
}

#ifdef _DEBUG

/*##**********************************************************************\
 *
 *      initializeTiming
 *
 * Tool for software performance analysis: initialize the timing globals.
 * Note that this is only used when measuring the TATP package performance
 * and for checking that there are no bottlenecks in the TATP package that
 * would interfere the actual performance testing
 *
 */
void initializeTiming()
{
        int i;

        for (i = 0; i < MAX_NUMBER_OF_TIMING_POINTS; i++) {
            timeForAnalysis[i] = 0;
            plusOrMinusTime[i] = 0; /* 0 means minus, 1 means plus.
                                       We start with minus. */
        }
}

/*##**********************************************************************\
 *
 *      timeMe
 *
 * Tool for software performance analysis: add or subtract the timer
 * to the memory. Note that this is only used when measuring the TATP
 * package performance and for checking that there are no bottlenecks in
 * the TATP package that would interfere the actual performance testing
 *
 * Parameters :
 *      id
 *          ID number of the procedure point to be measured.
*/
void timeMe(int id)
{
        __int64 tickerValue;

        getSystemTicker(&tickerValue);
        if (plusOrMinusTime[id]) {
            timeForAnalysis[id] += tickerValue;
            plusOrMinusTime[id] = 0;
        }
        else {
            timeForAnalysis[id] -= tickerValue;
            plusOrMinusTime[id] = 1;
        }
}

/*##**********************************************************************\
 *
 *      saveMyTimings
 *
 * Tool for software performance analysis: dump the timing memory to the disk.
 * Note that this is only used when measuring the TATP package performance and
 * for checking that there are no bottlenecks in the TATP package that would
 * interfere the actual performance testing
 *
 * Parameters :
 *      filename
 *          File name where data is saved.
 */
void saveMyTimings(char *filename)
{
        FILE *fp;
        int i;
        int allZeros;

        /* If all results all zero (meaning the timing feature */
        /* is not used), do not write the file */
        allZeros = 1;
        for (i = 0; i < MAX_NUMBER_OF_TIMING_POINTS; i++) {
            if (timeForAnalysis[i] != 0) {
                allZeros = 0;
                break;
            }
        }
        if (!allZeros) {
            openFileForWrite(&fp, filename);
            if (fp == NULL) {
                message('W',
                        "Cannot open the timing analysis file for "
                        "appending the timing data.");
            } else {
                for (i = 0; i < MAX_NUMBER_OF_TIMING_POINTS; i++) {
                    fprintf(fp, "%d;%ld\n", i, (long)timeForAnalysis[i]);
                    if (plusOrMinusTime[i] != 0) {
                        /* We must have paired checking points, so this check */
                        fprintf(fp, "Error in the previous measurement.\n");
                    }
                }
                fclose(fp);
            }
        }
}
#endif /* _DEBUG */
