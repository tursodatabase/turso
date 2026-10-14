/**********************************************************************\
**  source       * util.h
**  description  * Utility functions that are intended for common use 
**                 in the TATP package
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public 
**  License 1.0 as published by the Open Source Initiative (OSI). 
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/
 
#ifndef UTIL_H
#define UTIL_H

#include <stdio.h>
#include <errno.h>
#include <sys/types.h>

#include "tatp.h"

#include "server.h"

#ifdef WIN32
#include "pcre.h"
#include "pcreposix.h" 
#else
#include <regex.h>
/* In case of other operating systems, use OS specific directives 
(specified in the makefile) and an OS specific header */
#include "linuxspec.h"
#endif

#ifdef WIN32
#define CH_CWD(pDir) _chdir(pDir)
#define GET_CWD(pDir,size) _getcwd(pDir,size)
#define PATH_SEPARATOR "\\"
#else
#define CH_CWD(pDir) chdir(pDir)
#define GET_CWD(pDir,size) getcwd(pDir,size)
#define PATH_SEPARATOR "/"
#endif

/* the number of connection retrials to the database (in ConnectDB()) */
#define DB_CONNECTION_RETRIES 5

#define NUM_DB_TYPES 3
typedef enum db_type {
    DB_GENERIC = 0,
    DB_SOLID = 1,
    DB_INFORMIX = 2
} dbtype;

typedef struct {
    /* Log globals. Initialized by initializeLog() */
    FILE *fLog;          /* Log file */
    int verbose;         /* Verbose level */
    long warningCount;   /* Count of warning messages */
    long errorCount;     /* Count of normal and fatal error messages */
    char moduleName[14]; /* Module identifier to print with the log message */
    int moduleColor;     /* Color for console print */
} log_t;

/* SQL error checking functions */
int error_c (SQLHDBC hdbc, RETCODE err);
int error_s (SQLHSTMT hstmt, RETCODE err, const char *accepted_state);
int get_error(SQLHSTMT hstmt, char *szSqlState, char *szErrorMsg, 
              SDWORD *naterr);

/* create directory with given path */
int mkFullDirStructure(const char *path);

/* Log messages to file and console */
/* retrieving log object interface */
extern log_t* getLogObject();
/* Initialize the log */
int initializeLog(int verbose, const char *module, int color);  
/* Close log file */
int finalizeLog();  
/* Create the log file in append mode and set the global fLog */
int createLog(const char *LogFileName);   
/* Write messages to the log file and console */
int writeLog(const char type, const char *msg);
/* Simple wrapper that calls writeLog() */
int message(const char type, char *msg); 
void colorprint(const char *buffer, int ForeColor, int BackColor);

/* CRC checksum related methods */
/* Reflects CRC bits in the lookup table */
ULONG CRC32Reflect(ULONG ref, char ch);
/* Builds the CRC Lookup table array */
void CRC32InitTable(ULONG *CRCTable); 

/* Sleep some milliseconds */
void msSleep(int milliseconds);

/* Database connection handling */
int ConnectDB(SQLHENV *env, SQLHDBC *dbc, char *db_connect, 
              char *db_name);
int DisconnectDB(SQLHENV *env, SQLHDBC *dbc, char *db_name);

/* Returns minimum of two integers */
int minimum(int a, int b);

/* Opens a file */
int openFile(FILE **fp, char *filename);
int openFileForWrite(FILE **fp, char *filename);
int openFileWithPath(FILE **fp, char *filename, char *path);
int createFileInSequence(FILE **fp, char *filename);
int moveFileInSequence(char *filename);

/* Resolves target DB type */
RETCODE detectTargetDB (SQLHDBC* testdb, dbtype* db,
                        char *version, int printvalues);

int getTargetDBVersion(struct server_t **server,
                       char *ConnectString, char *version);

/* Copy a text file between two given locations */
int copyFile(char *sourceFile, char *targetFile);

/* Process a SQL file */
int processSQLFile(char *DBInitFileName, SQLHDBC *targetdb, 
		   struct server_t **server, char *connect_string);

/* Process a SQL file */
int processSQL(char *sql, SQLHDBC *targetdb, 
               struct server_t **server, char *connect_string);

/* File reading functions */
int readFileLine(FILE *file, char* buf, int bufLen);

/* String handling functions */
void removeComment(char* line);
int removeExtraWhitespace (char *string, int bufLen);
int removeEscapeCharacters (char *string, int bufLen);
int trim (char* string);
int composeSQLCommand(char* line, char** commandBuf);
int isEmptyBuf(const char *buf, size_t bufLen);

/* alter FD_CLOEXEC flag of a file */
int set_FD_cloexec_flag(int fd_no, int value);

/* regexp matching functions */
int simpleMatch(regex_t *r, char *line, char *pattern, 
                int *start, int *length);
int multiMatch(regex_t *r, char *line, char *pattern, 
               int *count, int *start, int *length);
int fullMatch(regex_t *r, char *line, char *pattern);
int fullMatch_e(regex_t *r, char *line, char *pattern, 
                int *count, int *starts, int *lengths);

/* Parameter reading utility functions */
int extractStringKeyword(const char *line, const char *keyword, 
			 char *value, int valueLen);
int extractIntKeyword(const char *line, const char *keyword, 
		      int *value);

#ifdef _DEBUG

/* Globals for the software (TATP) performance analysis */
#define MAX_NUMBER_OF_TIMING_POINTS 20
/* Cumulative times (tickers) */
extern __int64 timeForAnalysis[MAX_NUMBER_OF_TIMING_POINTS]; 
/* 0 means minus, 1 means plus */
extern int plusOrMinusTime[MAX_NUMBER_OF_TIMING_POINTS];     
 
/* Methods for the software (TATP) performance analysis */
void initializeTiming();
void timeMe(int id);
void saveMyTimings(char *filename);

#endif /* _DEBUG */

#define CHAR2SQL(s) \
    ((SQLCHAR*) (s))

#endif /* UTIL_H */
