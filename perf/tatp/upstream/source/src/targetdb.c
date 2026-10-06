/**********************************************************************\
**  source       * targetdb.c
**  description  * TargetDB related functions
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
#include "targetdb.h"

/* The names of the TATP tables */
static char table_names[4][17] = {
        "subscriber",
        "access_info",
        "special_facility",
        "call_forwarding"
};

/* The column lists for all the TATP tables */
static char subscriber_fields[34][13] = {
        "s_id",
        "sub_nbr",
        "bit_1",
        "bit_2",
        "bit_3",
        "bit_4",
        "bit_5",
        "bit_6",
        "bit_7",
        "bit_8",
        "bit_9",
        "bit_10",
        "hex_1",
        "hex_2",
        "hex_3",
        "hex_4",
        "hex_5",
        "hex_6",
        "hex_7",
        "hex_8",
        "hex_9",
        "hex_10",
        "byte2_1",
        "byte2_2",
        "byte2_3",
        "byte2_4",
        "byte2_5",
        "byte2_6",
        "byte2_7",
        "byte2_8",
        "byte2_9",
        "byte2_10",
        "msc_location",
        "vlr_location"
};
static char access_info_fields[6][8] = {
        "s_id",
        "ai_type",
        "data1",
        "data2",
        "data3",
        "data4"
};
static char special_facility_fields[6][12] = {
        "s_id",
        "sf_type",
        "is_active",
        "error_cntrl",
        "data_a",
        "data_b"
};
static char call_forwarding_fields[5][11] = {
        "s_id",
        "sf_type",
        "start_time",
        "end_time",
        "numberx"
};

/*##**********************************************************************\
 *
 *      initializeTargetDatabase
 *
 * Run commands specified in the database schema file (directive
 * targetDBSchema in tm.ini) against the target database.
 * If the command is "CREATE TABLE..." the command (table schema)
 * is checked prior executing it in the database.
 * If the command is "DROP TABLE..." the return value from the
 * database is ignored (to avoid reporting 'table does not exist'
 * errors)
 *
 * Parameters:
 *      testdb 
 *          database to connect to
 *
 *      DBSchemaFileName
 *          database schema initialization file
 *
 *      DBSchemaName 
 *          database schema name
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int initializeTargetDatabase(SQLHDBC testdb, char *DBSchemaFileName,
                             char *DBSchemaName)
{
        char msg[W_EL];
        char line[W_EL];	    /* read buffer */
        FILE *fSchemaFile;	    /* Database schema file to open */
        RETCODE err;
        SQLHSTMT statement;
        char *cmd;			    /* The database (SQL) command buffer */
        char commandHead[W];    /* the start of an SQL command, e.g.,
                                   "create table" */
        int stat, firstLineWithinACommand;
        int ind, dropTableCmd;
        
        err = 0;
        firstLineWithinACommand = 1;
        statement = 0;
        
        if (openFile(&fSchemaFile, DBSchemaFileName) != 0) {
            /* This should not actually happen, because the file
               was checked earlier */
            sprintf(msg, "Cannot open DB schema file '%s'", DBSchemaFileName);
            message('F', msg);
            return E_FATAL; /* Fatal error */
        }
        
        /* Parse the file */
        while (readFileLine(fSchemaFile, line, W_EL) != -1) {
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
            if (stat == E_FATAL) {
                /* an error occurred */
                return E_FATAL;
            }
            else if (stat == 1) {
                /* A full SQL command read (=until a semicolon),
                   -> process it */
                dropTableCmd = 0;
                sprintf(msg, "Executing command '%s'", cmd);
                message('D', msg);
                if (strlen(cmd) >= 12) {
                    strncpy(commandHead, cmd, 12);
                    for (ind = 0; ind < 12; ind++) {
                        /* Convert the command start to upper case for
                           comparison with CREATE TABLE and DROP TABLE */
                        commandHead[ind] = toupper(commandHead[ind]);
                    }
                    if (strncmp(commandHead, "CREATE TABLE", 12) == 0) {
                        /* if the table is one of the mandatory TATP tables,
                           check its validity (by checking its columns) */
                        if (checkTableDefinition(cmd, DBSchemaName)
                            == E_FATAL) {
                            err = E_FATAL;
                        }
                    }
                    else if (strncmp(commandHead, "DROP TABLE", 10) == 0) {
                        dropTableCmd = 1;
                    }
                }
                if (!err) {
                    err = SQLAllocHandle(SQL_HANDLE_STMT, testdb, &statement);
                    if (error_c(testdb, err) != 0) {
                        message('E', "SQLAllocHandle failed");
                        err = E_FATAL;
                    }
                }
                if (!err) {
                    err = SQLExecDirect(statement, CHAR2SQL(cmd), SQL_NTS);
                    if (dropTableCmd == 0) {
                        /* The status of the command is not checked if
                           the command was a DROP TABLE command
                           <- we ignore errors returned by DROP TABLE
                           commands */
                        if (error_s(statement, err, NULL) != 0) {
                            message('E', "SQLExecute failed");
                            message('D', cmd);
                        }
                    }
                    /* not a fatal error */
                    err = 0;
                }
                if (!err) {
                    err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
                    if (error_s(statement, err, NULL)) {
                        message('E', "SQLFreeHandle failed");
                        /* not a fatal error */
                        err = 0;
                    }
                }
                if (!err) {
                    err = SQLEndTran(SQL_HANDLE_DBC, testdb, SQL_COMMIT);
                    if (error_c(testdb, err)) {
                        message('E', "SQLEndTran failed");
                        /* not a fatal error */
                        err = 0;
                    }
                }
                free(cmd);
                /* For the next command */
                firstLineWithinACommand = 1;
                if (err == E_FATAL) {
                    /* fatal error, stop processing. In case of non-fatal
                       errors we still try to process the rest of
                       the schema file */
                    fclose(fSchemaFile);
                    return err;
                }
            }
        } /* while readFileLine */
        
        message('D', "Database schema file processed.");
        
        /* close the database initialization file */
        fclose(fSchemaFile);
        return 0;
}

/*##**********************************************************************\
 *
 *      emptyTATPTables
 *
 * Empties the tables SUBSCRIBER, ACCESS_INFO, SPECIAL_FACILITY
 * and CALL_FORWARDING that are all TATP tables. If a DELETE command
 * returns an error the error is logged and the next command is executed.
 *
 * Parameters:
 *      testdb 
 *		    the target database
 *
 *      DBSchemaName
 *          database schema name
 *
 * Return value:
 *      0  - success
 *      E_FATAL or E_ERROR  - error
 */
int emptyTATPTables(SQLHDBC testdb, char *DBSchemaName)
{
        /* make sure that TATP tables are empty (some of them could
           be created earlier) */
        RETCODE err;
        SQLHSTMT statement;
        char *cmd = NULL;    /* The database (SQL) command buffer */
        int i;

        message('D', "Emptying TATP tables");

        cmd = (char*)calloc(29, sizeof(char));
        if (cmd == NULL) {
            message('F', "Dynamic memory allocation failed.");
            return E_FATAL;
        }
        for (i = 0; i < 4; i++) {
            /* Empty the four TATP tables listed in the switch
               clause below */
            err = SQLAllocHandle(SQL_HANDLE_STMT, testdb, &statement);
            if (error_c(testdb, err) != 0) {
                message('E', "SQLAllocHandle failed");
                free(cmd);
                return E_ERROR;
            }

            sprintf(cmd, "DELETE FROM %s%s",
                    (*DBSchemaName == '\0' ? "" : DBSchemaName),
                    table_names[3-i]);

            err = SQLExecDirect(statement, CHAR2SQL(cmd), SQL_NTS);

            if (err == SQL_NO_DATA) {
                err = SQL_SUCCESS;
            }

            if (error_s(statement, err, NULL) != 0) {
                message('E', "SQLExecute failed");
                message('E', cmd);
            }

            err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
            if (error_s(statement, err, NULL)) {
                message('E', "SQLFreeHandle failed");
            }

            err = SQLEndTran(SQL_HANDLE_DBC, testdb, SQL_COMMIT);
            if (error_c(testdb, err)) {
                message('E', "SQLEndTran failed");
            }
        }
        /* everything OK */
        message('D', "TATP test tables have been emptied.");
        free(cmd);
        return 0;
}

/*##**********************************************************************\
 *
 *      checkTableDefinition
 *
 * Checks that a TATP table defined by the user in the schema
 * definition file is correct. Checks that mandatory columns are defined
 * and that they have the right type.
 *
 * Parameters:
 *      cmd 
 *          the SQL command
 *
 *      DBSchemaName 
 *          database schema name
 *
 * Return value:
 *      E_OK     - success
 *      E_FATAL  - error
 */
int checkTableDefinition(char* cmd, char* DBSchemaName)
{
        int slen = 0;
        int i;
        int err = E_OK;
        char* str;
        if (*DBSchemaName != '\0') {
            slen += 1+strlen(DBSchemaName);
        }

        str = (char*)malloc((strlen(cmd)+1)*sizeof(char));
        if (str == NULL) {
            message('F', "Dynamic memory allocation failed ..");
            return E_FATAL;
        }
        for (i = 0; i < strlen(cmd) ; i++) {
            str[i] = tolower(cmd[i]);
        }
        str[strlen(cmd)]= '\0';

        if (strncmp(cmd+13+slen, table_names[TABLENAME_POS_SUBSCRIBER],
                    strlen(table_names[TABLENAME_POS_SUBSCRIBER])) == 0) {
            /* The subscriber table definition */
            if (strstr(str, "s_id integer") == 0) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 's_id')");
                err = E_FATAL;
            }
            if (strstr(str, "sub_nbr varchar") == 0) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'sub_nbr')");
                err = E_FATAL;
            }
            if ((strstr(str, "bit_1 tinyint") == 0)
                && (strstr(str, "bit_1 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'bit_1')");
                err = E_FATAL;
            }
            if ((strstr(str, "bit_2 tinyint") == 0)
                && (strstr(str, "bit_2 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'bit_2')");
                err = E_FATAL;
            }
            if ((strstr(str, "bit_3 tinyint") == 0)
                && (strstr(str, "bit_3 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'bit_3')");
                err = E_FATAL;
            }
            if ((strstr(str, "bit_4 tinyint") == 0)
                && (strstr(str, "bit_4 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'bit_4')");
                err = E_FATAL;
            }
            if ((strstr(str, "bit_5 tinyint") == 0)
                && (strstr(str, "bit_5 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'bit_5')");
                err = E_FATAL;
            }
            if ((strstr(str, "bit_6 tinyint") == 0)
                && (strstr(str, "bit_6 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'bit_6')");
                err = E_FATAL;
            }
            if ((strstr(str, "bit_7 tinyint") == 0)
                && (strstr(str, "bit_7 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'bit_7')");
                err = E_FATAL;
            }
            if ((strstr(str, "bit_8 tinyint") == 0)
                && (strstr(str, "bit_8 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'bit_8')");
                err = E_FATAL;
            }
            if ((strstr(str, "bit_9 tinyint") == 0)
                && (strstr(str, "bit_9 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'bit_9')");
                err = E_FATAL;
            }
            if ((strstr(str, "bit_10 tinyint") == 0)
                && (strstr(str, "bit_10 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'bit_10')");
                err = E_FATAL;
            }
            if ((strstr(str, "hex_1 tinyint") == 0)
                && (strstr(str, "hex_1 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'hex_1')");
                err = E_FATAL;
            }
            if ((strstr(str, "hex_2 tinyint") == 0)
                && (strstr(str, "hex_2 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'hex_2')");
                err = E_FATAL;
            }
            if ((strstr(str, "hex_3 tinyint") == 0)
                && (strstr(str, "hex_3 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'hex_3')");
                err = E_FATAL;
            }
            if ((strstr(str, "hex_4 tinyint") == 0)
                && (strstr(str, "hex_4 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'hex_4')");
                err = E_FATAL;
            }
            if ((strstr(str, "hex_5 tinyint") == 0)
                && (strstr(str, "hex_5 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'hex_5')");
                err = E_FATAL;
            }
            if ((strstr(str, "hex_6 tinyint") == 0)
                && (strstr(str, "hex_6 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'hex_6')");
                err = E_FATAL;
            }
            if ((strstr(str, "hex_7 tinyint") == 0)
                && (strstr(str, "hex_7 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'hex_7')");
                err = E_FATAL;
            }
            if ((strstr(str, "hex_8 tinyint") == 0)
                && (strstr(str, "hex_8 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'hex_8')");
                err = E_FATAL;
            }
            if ((strstr(str, "hex_9 tinyint") == 0)
                && (strstr(str, "hex_9 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'hex_9')");
                err = E_FATAL;
            }
            if ((strstr(str, "hex_10 tinyint") == 0)
                && (strstr(str, "hex_10 smallint") == 0)) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'hex_10')");
                err = E_FATAL;
            }
            if (strstr(str, "byte2_1 smallint") == 0) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'byte2_1')");
                err = E_FATAL;
            }
            if (strstr(str, "byte2_2 smallint") == 0) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'byte2_2')");
                err = E_FATAL;
            }
            if (strstr(str, "byte2_3 smallint") == 0) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'byte2_3')");
                err = E_FATAL;
            }
            if (strstr(str, "byte2_4 smallint") == 0) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'byte2_4')");
                err = E_FATAL;
            }
            if (strstr(str, "byte2_5 smallint") == 0) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'byte2_5')");
                err = E_FATAL;
            }
            if (strstr(str, "byte2_6 smallint") == 0) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'byte2_6')");
                err = E_FATAL;
            }
            if (strstr(str, "byte2_7 smallint") == 0) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'byte2_7')");
                err = E_FATAL;
            }
            if (strstr(str, "byte2_8 smallint") == 0) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'byte2_8')");
                err = E_FATAL;
            }
            if (strstr(str, "byte2_9 smallint") == 0) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'byte2_9')");
                err = E_FATAL;
            }
            if (strstr(str, "byte2_10 smallint") == 0) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'byte2_10')");
                err = E_FATAL;
            }
            if (strstr(str, "msc_location integer") == 0) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'msc_location')");
                err = E_FATAL;
            }
            if (strstr(str, "vlr_location integer") == 0) {
                message('E', "Schema file error (Table: 'subscriber' "
                        "Column: 'vlr_location')");
                err = E_FATAL;
            }
        }
        else if (strncmp(cmd+13+slen,
                         table_names[TABLENAME_POS_ACCESSINFO],
                         strlen(table_names[TABLENAME_POS_ACCESSINFO]))
                 == 0) {
            /* The access_info table definition */
            if (strstr(str, "s_id integer") == 0) {
                message('E', "Schema file error (Table: 'access_info' "
                        "Column: 's_id')");
                err = E_FATAL;
            }
            if ((strstr(str, "ai_type tinyint") == 0)
                && (strstr(str, "ai_type smallint") == 0)) {
                message('E', "Schema file error (Table: 'access_info' "
                        "Column: 'ai_type')");
                err = E_FATAL;
            }
            if (strstr(str, "data1 smallint") == 0) {
                message('E', "Schema file error (Table: 'access_info' "
                        "Column: 'data1')");
                err = E_FATAL;
            }
            if (strstr(str, "data2 smallint") == 0) {
                message('E', "Schema file error (Table: 'access_info' "
                        "Column: 'data2')");
                err = E_FATAL;
            }
            if (strstr(str, "data3 char(3)") == 0) {
                message('E', "Schema file error (Table: 'access_info' "
                        "Column: 'data3')");
                err = E_FATAL;
            }
            if (strstr(str, "data4 char(5)") == 0) {
                message('E', "Schema file error (Table: 'access_info' "
                        "Column: 'data4')");
                err = E_FATAL;
            }
            if (strstr(str, "primary key (s_id, ai_type)") == 0) {
                message('E', "Schema file error (Table: 'access_info' "
                        "Location: Primary key setting)");
                err = E_FATAL;
            }
            if (strstr(str,
                       "foreign key (s_id) references subscriber (s_id)")
                == 0) {
                message('E', "Schema file error (Table: 'access_info' "
                        "Location: Foreign key setting)");
                err = E_FATAL;
            }
        }
        else if (strncmp(cmd+13+slen,
                         table_names[TABLENAME_POS_SPECIALFACILITY],
                         strlen(table_names[TABLENAME_POS_SPECIALFACILITY]))
                 == 0)
        {
            /* The special_facility table definition */
            if (strstr(str, "s_id integer") == 0) {
                message('E', "Schema file error (Table: 'special_facility' "
                        "Column: 's_id')");
                err = E_FATAL;
            }
            if ((strstr(str, "sf_type tinyint") == 0)
                && (strstr(str, "sf_type smallint") == 0)) {
                message('E', "Schema file error (Table: 'special_facility' "
                        "Column: 'sf_type')");
                err = E_FATAL;
            }
            if ((strstr(str, "is_active tinyint") == 0)
                && (strstr(str, "is_active smallint") == 0)) {
                message('E', "Schema file error (Table: 'special_facility' "
                        "Column: 'is_active')");
                err = E_FATAL;
            }
            if (strstr(str, "error_cntrl smallint") == 0) {
                message('E', "Schema file error (Table: 'special_facility' "
                        "Column: 'error_cntrl')");
                err = E_FATAL;
            }
            if (strstr(str, "data_a smallint") == 0) {
                message('E', "Schema file error (Table: 'special_facility' "
                        "Column: 'data_a')");
                err = E_FATAL;
            }
            if (strstr(str, "data_b char(5)") == 0) {
                message('E', "Schema file error (Table: 'special_facility' "
                        "Column: 'data_b')");
                err = E_FATAL;
            }
            if (strstr(str, "primary key (s_id, sf_type)") == 0) {
                message('E', "Schema file error (Table: 'special_facility' "
                        "Location: Primary key setting)");
                err = E_FATAL;
            }
            if (strstr(str, "foreign key (s_id) references subscriber "
                       "(s_id)") == 0) {
                message('E', "Schema file error (Table: 'special_facility' "
                        "Location: Foreign key setting)");
                err = E_FATAL;
            }
        }
        else if (strncmp(cmd+13+slen, table_names[TABLENAME_POS_CALLFORWARDING],
                         strlen(table_names[TABLENAME_POS_CALLFORWARDING]))
                 == 0) {
            /* The special_facility table definition */
            if (strstr(str, "s_id integer") == 0) {
                message('E', "Schema file error (Table: 'call_forwarding' "
                        "Column: 's_id')");
                err = E_FATAL;
            }
            if ((strstr(str, "sf_type tinyint") == 0)
                && (strstr(str, "sf_type smallint") == 0)) {
                message('E', "Schema file error (Table: 'call_forwarding' "
                        "Column: 'sf_type')");
                err = E_FATAL;
            }
            if ((strstr(str, "start_time tinyint") == 0)
                && (strstr(str, "start_time smallint") == 0)) {
                message('E', "Schema file error (Table: 'call_forwarding' "
                        "Column: 'start_time')");
                err = E_FATAL;
            }
            if ((strstr(str, "end_time tinyint") == 0)
                && (strstr(str, "end_time smallint") == 0)) {
                message('E', "Schema file error (Table: 'call_forwarding' "
                        "Column: 'end_time')");
                err = E_FATAL;
            }
            if (strstr(str, "numberx varchar(15)") == 0) { /* SUBNBR_LENGTH */
                message('E', "Schema file error (Table: 'call_forwarding' "
                        "Column: 'numberx')");
                err = E_FATAL;
            }
            if (strstr(str, "primary key (s_id, sf_type, start_time)") == 0) {
                message('E', "Schema file error (Table: 'call_forwarding' "
                        "Location: Primary key setting)");
                err = E_FATAL;
            }
            if (strstr(str, "foreign key (s_id, sf_type) references "
                       "special_facility(s_id, sf_type)") == 0) {
                message('E', "Schema file error (Table: 'call_forwarding' "
                        "Location: Foreign key setting)");
                err = E_FATAL;
            }
        }
        free(str);
        return err;
}


/*##**********************************************************************\
 *
 *      checkTableSchema
 *
 * Checks that the database contains all mandatory tables. Also the
 * existence of mandatory columns and their types are checked.
 *
 * Parameters:
 *      testdb 
 *          target database handle
 *
 *      DBSchemaName
 *          database schema name
 *
 * Return value:
 *      E_OK - success
 *      E_FATAL - error
 */
int checkTableSchema(SQLHDBC testdb, char *DBSchemaName)
{
        char        cmd[512];
        char        msg[512];
        RETCODE     err;
        RETCODE     err2;
        SQLHSTMT    statement;
        int         i;
        char        column_name[128];
        char        buf[128];
        SQLINTEGER  column_size;
        SQLLEN      size1;
        SQLLEN      size2;
        SQLLEN      size3;
        SQLSMALLINT data_type;

        /* check tables */
        message('I', "Checking TATP tables");
        if (*DBSchemaName != '\0') {
            sprintf(buf, "Using tables under schema '%s'.", DBSchemaName);
            message('I', buf);
        }
        /* Issue a select command against every TATP table and project
           those column mandatory for TATP. If we get an error a
           mandatory column is missing */
#ifdef NO_TPS_TABLE
        for (i = 0; i < 4; i++) {
#else            
        for (i = 0; i < 5; i++) {
#endif            
            switch (i) {
                case 0:
                    /* the SUBSCRIBER table */
                    sprintf(cmd, "SELECT s_id, sub_nbr, bit_1, bit_2, bit_3, "
                            "bit_4, bit_5, bit_6, bit_7, bit_8, bit_9, bit_10, "
                            "hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7, "
                            "hex_8, hex_9, hex_10, byte2_1, byte2_2, byte2_3, "
                            "byte2_4, byte2_5, byte2_6, byte2_7, byte2_8, "
                            "byte2_9, byte2_10, msc_location, vlr_location "
                            "FROM %s%s",
                            (*DBSchemaName == '\0' ? "" : DBSchemaName),
                            table_names[TABLENAME_POS_SUBSCRIBER]);
                    break;
                case 1:
                    /* the ACCESS_INFO table */
                    sprintf(cmd, "SELECT s_id, ai_type, data1, data2, data3, "
                            "data4 FROM %s%s",
                            (*DBSchemaName == '\0' ? "" : DBSchemaName),
                            table_names[TABLENAME_POS_ACCESSINFO]);
                    break;
                case 2:
                    /* the SPECIAL_FACILITYtable */
                    sprintf(cmd, "SELECT s_id, sf_type, is_active, "
                            "error_cntrl, data_a, data_b FROM %s%s",
                            (*DBSchemaName == '\0' ? "" : DBSchemaName),
                            table_names[TABLENAME_POS_SPECIALFACILITY]);
                    break;
                case 3:
                    /* the CALL_FORWARDING table */
                    sprintf(cmd, "SELECT s_id, sf_type, start_time, end_time, "
                            "numberx FROM %s%s",
                            (*DBSchemaName == '\0' ? "" : DBSchemaName),
                            table_names[TABLENAME_POS_CALLFORWARDING]);
                    break;
#ifndef NO_TPS_TABLE
                case 4:
                    /* the TPS table */
                    sprintf(cmd, "SELECT id, value FROM %stps",
                            (*DBSchemaName == '\0' ? "" : DBSchemaName));
                    break;
#endif
            }
            err = SQLAllocHandle(SQL_HANDLE_STMT, testdb, &statement);
            if (error_c(testdb, err) != 0) {
                message('E', "SQLAllocHandle failed");
                return E_FATAL;
            }
            err = SQLExecDirect(statement, CHAR2SQL(cmd), SQL_NTS);
            if (error_s(statement, err, NULL) != 0) {
                message('E', "TATP table check failed");
                message('D', cmd);
                return E_FATAL;
            }
            err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
            if (error_s(statement, err, NULL)) {
                message('E', "SQLFreeHandle failed");
                return E_FATAL;
            }
        }

        /* Check also that the column types are valid for the
           TATP benchmark*/
        err = SQLAllocHandle(SQL_HANDLE_STMT, testdb, &statement);
        if (error_c(testdb, err) != 0) {
            message('E', "SQLAllocHandle failed");
            return E_FATAL;
        }
        err2 = 0;
        for (i = 0; i < 4; i++) {
            /* Iterate through the TATP tables */

            err = SQLColumns(statement,
                             NULL, 0,                      /* all catalogs */
                             NULL, 0,                      /* all schemas */
                             CHAR2SQL(table_names[i]),     /* table */
                             SQL_NTS,
                             NULL, 0);                     /* all columns */

            if (error_s(statement, err, NULL) != 0) {
                message('E', "SQLColumns failed");
                message('D', cmd);
                err = E_FATAL;
            }

            if (!err) {
                /* bind the column name column */
                err = SQLBindCol(statement,
                                 4,
                                 SQL_C_CHAR,
                                 column_name,
                                 sizeof(column_name),
                                 &size1);

                if (err != SQL_SUCCESS && err != SQL_SUCCESS_WITH_INFO) {
                    /* an error */
                    message('E', "SQLBindCol failed");
                    return E_FATAL;
                }
                /* bind the column data type column */
                err = SQLBindCol(statement,
                                 5,
                                 SQL_C_SSHORT,
                                 &data_type,
                                 0,
                                 &size2);

                if (err != SQL_SUCCESS && err != SQL_SUCCESS_WITH_INFO) {
                    /* an error */
                    message('E', "SQLBindCol failed");
                    return E_FATAL;
                }
                /* bind the column size column */
                err = SQLBindCol(statement,
                                 7,
                                 SQL_C_SLONG,
                                 &column_size,
                                 0,
                                 &size3);

                if (err != SQL_SUCCESS && err != SQL_SUCCESS_WITH_INFO) {
                    /* an error */
                    message('E', "SQLBindCol failed");
                    return E_FATAL;
                }
                while ((err = SQLFetch(statement)) != SQL_NO_DATA) {
                    /* Check the type information of all the columns
                       of a table */

                    if (error_s(statement, err, NULL) != 0) {
                        message('E', "SQLFetch failed");
                        message('D', cmd);
                        return E_FATAL;
                    }

                    /* check field type */
                    if (checkColumnType(column_name, data_type) != 0) {
                        /* error */
                        sprintf(msg,
                                "Invalid data type. Table: '%s'  Column: '%s'",
                                table_names[i], column_name);
                        message('E', msg);
                        err2 = E_ERROR;
                    }
                    /* check field length */
                    if ((data_type == SQL_VARCHAR)
                        || (data_type == SQL_CHAR)) {
                        if (column_size != getColumnSize(column_name)) {
                            sprintf(msg,
                                    "The column has invalid size. Table: '%s' "
                                    "Column: '%s'",
                                    table_names[i], column_name);
                            message('E', msg);
                            err2 = E_ERROR;
                        }
                    }
                }
                err = SQLCloseCursor(statement);
                if (error_s(statement, err, NULL)) {
                    writeLog('E', "SQLCloseCursor failed");
                }
            }
        }

        /* clean up */
        err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
        if (error_s(statement, err, NULL)) {
            message('E', "SQLFreeHandle failed");
            return E_FATAL;
        }

        if (err2 == 0) {
            /* Everything OK (no wrong column type). If a column type
               did not match the TATP requirements, a warning was given.
               Even in that case we continue execution .*/
            message('D', "Table schema is OK.");
        }
        return E_OK;
}

/*##**********************************************************************\
 *
 *      checkTargetDatabase
 *
 * Checks that the target database contains all mandatory tables
 * and that subscribers table has correct number of rows.
 *
 * Parameters:
 *      cmd 
 *          command in tdf file
 *      testdb
 *          target database
 *      expected_size
 *          expected number of the rows in the subscriber table
 *      DBSchemaName
 *          database schema name
 *
 * Return value:
 *      0  - success
 *     !0  - target database schema was invalid or the database
 *           was not correctly populated
 */
int checkTargetDatabase(cmd_type cmd, SQLHDBC testdb, int expected_size,
                        char* DBSchemaName)
{
        RETCODE err = 0;

        if (cmd == POPULATE || cmd == POPULATE_CONDITIONALLY) {
            /* Check table schema */
            err = (RETCODE)checkTableSchema(testdb, DBSchemaName);
            if (err != 0) {
                message('D', "Target database table schema is invalid");
            }
        }
        if (!err) {
            err = (RETCODE)checkTargetDBpopulation(testdb, expected_size,
                                                   DBSchemaName);
        }
        return err;

}

/*##**********************************************************************\
 *
 *      checkTargetDBpopulation
 *
 * Checks that the subscribers table has correct number of rows.
 *
 * Parameters:
 *      testdb
 *          db handle
 *
 *      expected_size
 *          expected number of the rows in the subscriber table
 *
 *      DBSchemaName
 *          database schema name
 *
 * Return value:
 *      E_OK  - success
 *      E_FATAL  - target database was not correctly populated
 */
int checkTargetDBpopulation(SQLHDBC testdb, int expected_size,
                            char *DBSchemaName)
{
        char       msg[256];
        RETCODE    err;
        char       cmd[64];
        int        count;
        SQLHSTMT   statement;

        /* check subscriber table population */
        sprintf(msg,
                "Checking TATP population (%d subscribers)",
                expected_size);
        message('I', msg);

        sprintf(cmd, "SELECT COUNT(*) FROM %s%s",
                (*DBSchemaName == '\0' ? "" : DBSchemaName),
                table_names[TABLENAME_POS_SUBSCRIBER]);

        err = SQLAllocHandle(SQL_HANDLE_STMT, testdb, &statement);
        if (error_c(testdb, err) != 0) {
            message('E', "SQLAllocHandle failed");
            return E_FATAL;
        }
        /* Bind the result to 'count'*/
        err = SQLBindCol(statement,
                         1,
                         SQL_C_SLONG,
                         &count,
                         0,
                         NULL);
        if (error_c(testdb, err) != 0) {
            message('E', "SQLBindCol failed");
            return E_FATAL;
        }
        err = SQLPrepare(statement, CHAR2SQL(cmd), SQL_NTS);
        if (error_s(statement, err, NULL) != 0) {
            message('E', "SQLPrepare failed");
            message('D', cmd);
            return E_FATAL;
        }
        err = SQLExecute(statement);
        if (error_s(statement, err, NULL) != 0) {
            message('E', "SQLExecute failed");
            message('D', cmd);
            return E_FATAL;
        }
        err = SQLFetch(statement);
        if (error_s(statement, err, NULL) != 0) {
            message('E', "SQLFetch failed");
            message('D', cmd);
            return E_FATAL;
        }
        err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
        if (error_s(statement, err, NULL)) {
            message('E', "SQLFreeHandle failed");
            return E_FATAL;
        }
        if (count != expected_size) {
            sprintf(msg, "Wrong number of subscribers (%d) in target "
                    "database (expected: %d).",
                    count, expected_size);
            message('E', msg);
            return E_FATAL;
        }

        return E_OK;
}

/*##**********************************************************************\
 *
 *      populateDatabase
 *
 * Populates the benchmark database. Tables are supposed to
 * pre-exist before calling this function.
 *
 * Parameters:
 *      testdb 
 *		    database to populate
 *
 *      population_size 
 *		    size of the subscriber table (other sizes are based on that one)
 *
 *      populationCommitBlockRows 
 *		    amount of rows to insert between commits 0 = autocommit
 *
 *      seq_order_keys
 *          0 = insert rows in subscriber table in random order
 *          1 = insert rows in subscriber table in sequential order
 *
 *      DBSchemaName 
 *          Database schema name
 *
 *      min_subscriber_id
 *          Minimum S_ID value
 *
 *      max_subscriber_id
 *          Maximum S_ID value
 *
 * Return value:
 *      0 - success
 *     !0 - error 
 */
int populateDatabase(SQLHDBC testdb, int population_size,
                     int populationCommitBlockRows, int seq_order_keys,
                     char *DBSchemaName,
                     int min_subscriber_id,
                     int max_subscriber_id)
{
        int i, j, k, x;
        /* The statement handles */
        SQLHSTMT statement, statement2, statement3, statement4;
        char cmd[1024];
        RETCODE err;
        int ret;
        char sub_nbr[SUBNBR_LENGTH + 1];
        char msg[W_L];
        int sr_type[4];
        int sf_type[4];
        int *start_time = NULL;
        int sr = 0, st = 0;
        int s_id = 0, pos = 0, temp = 0;
        int param_length = 0;
        int subrecord_amount, callfwd_amount;
        int *s_ids = NULL;
        int param_value_subscriber[34];
        int param_value_accessinfo[2];
        char param_value_accessinfo_data3[AI_DATA3_LENGTH + 1];
        char param_value_accessinfo_data4[AI_DATA4_LENGTH + 1];
        int param_value_special[3];
        char param_value_special_str[AI_DATA4_LENGTH + 1];
        int param_value_callfwd[2];
        char param_value_callfwd_str[SUBNBR_LENGTH + 1];
        char subrstr[SUBNBR_LENGTH + 1];
        rand_t rand;

        int count_access_info = 0;
        int count_special_facility = 0;
        int count_call_forwarding = 0;
        
        int commitblock;
        int n_loops_after_commit = 0;

        int subscribers_to_populate = max_subscriber_id - min_subscriber_id + 1;

        if (subscribers_to_populate < populationCommitBlockRows) {
            commitblock = subscribers_to_populate;
        } else {
            commitblock = populationCommitBlockRows;
        }

        /* TODO */
        init_genrand(&rand, 5489UL - min_subscriber_id);

        statement = statement2 = statement3 = statement4 = 0;

        for (i = 0; i != SUBNBR_LENGTH; ++i)
            subrstr[i] = '0';

        subrstr[SUBNBR_LENGTH] = '\0';

        s_ids = (int*)calloc(subscribers_to_populate, sizeof(int));
        if (s_ids == NULL) {
            message('F', "Dynamic memory allocation failed.");
            return E_FATAL;
        }
        /* populate an array of IDs */
        for (i = 0; i < subscribers_to_populate; i++) {
            s_ids[i] = min_subscriber_id + i;
        }

        /* Do not insert identifiers in consecutive order */
        if (seq_order_keys == 0) {
            /* swap every element to produce an unsorted array */
            for (i = 0; i < subscribers_to_populate; i++) {
                pos = get_random(&rand, 0, subscribers_to_populate - 1);
                temp = s_ids[pos];
                s_ids[pos] = s_ids[i];
                s_ids[i] = temp;
            }
        }

        /* The main loop. In one iteration we insert
           - one subscriber row
           - 0-3 call_forwarding rows
           - 1-4 access_info rows
           - 1-4 special_facility rows
        */

        err = 0;
        
        for (i = 0; i < subscribers_to_populate; i++) {
            if (err) {
                SQLEndTran(SQL_HANDLE_DBC, testdb, SQL_ROLLBACK);
            }
            n_loops_after_commit++;
            
            s_id = s_ids[i];              /* subscriber id */

            sub_nbr_gen(s_id, sub_nbr);   /* sub_nbr SUBNBR_LENGTH
                                             number string */

            /* Allochandle & Prepare only once */
            if ((i == 0) && (err == 0)) {

                /* --- insert subscriber record --- */
                err = SQLAllocHandle(SQL_HANDLE_STMT, testdb, &statement);
                if (error_c(testdb, err) != 0) {
                    message('E', "SQLAllocHandle failed");
                    return E_ERROR;
                }

                sprintf(cmd, "INSERT INTO %s%s (s_id, sub_nbr, bit_1,"
                        "bit_2, bit_3, bit_4, bit_5, bit_6, bit_7, bit_8,"
                        "bit_9, bit_10, hex_1, hex_2, hex_3, hex_4, hex_5,"
                        "hex_6, hex_7, hex_8, hex_9, hex_10, byte2_1,"
                        "byte2_2, byte2_3, byte2_4, byte2_5, byte2_6,"
                        "byte2_7, byte2_8, byte2_9, byte2_10, msc_location,"
                        "vlr_location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
                        "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (*DBSchemaName == '\0' ? "" : DBSchemaName),
                        table_names[TABLENAME_POS_SUBSCRIBER]);
                err = SQLPrepare(statement, CHAR2SQL(cmd), SQL_NTS);

                if (error_s(statement, err, NULL) != 0) {
                    message('E', "SQLPrepare failed");
                    return E_ERROR;
                }

                err = SQLBindParameter(statement,
                                       1,
                                       SQL_PARAM_INPUT,
                                       SQL_C_SLONG,
                                       SQL_INTEGER,
                                       0,
                                       0,
                                       &s_id,
                                       0,
                                       NULL);

                if (error_s(statement, err, NULL)) {
                    message('E', "SQLBindParameter failed");
                    return E_ERROR;
                }

                err = SQLBindParameter(statement,
                                       2,
                                       SQL_PARAM_INPUT,
                                       SQL_C_CHAR,
                                       SQL_VARCHAR,
                                       SUBNBR_LENGTH,
                                       0,
                                       sub_nbr,
                                       0,
                                       NULL);

                if (error_s(statement, err, NULL)) {
                    message('E', "SQLBindParameter failed");
                    return E_ERROR;
                }

                /* Bind parameters to multiple columns in a loop */
                for (j = 2; j < 34; j++) {
                    /* bind to correct variable type */
                    err = SQLBindParameter(statement,
                                           (SQLUSMALLINT)(j+1),
                                           SQL_PARAM_INPUT,
                                           (SQLSMALLINT)
                                           getValueType(subscriber_fields[j]),
                                           (SQLSMALLINT)
                                           getParamType(subscriber_fields[j]),
                                           getColumnSize(subscriber_fields[j]),
                                           0,
                                           &param_value_subscriber[j],
                                           0,
                                           NULL);

                    if (error_s(statement, err, NULL)) {
                        message('E', "SQLBindParameter failed");
                        return E_ERROR;
                    }
                }

                /* --- insert access_info record --- */
                err = SQLAllocHandle(SQL_HANDLE_STMT, testdb, &statement2);
                if (error_c(testdb, err) != 0) {
                    message('E', "SQLAllocHandle failed");
                    return E_ERROR;
                }

                sprintf(cmd, "INSERT INTO %s%s (s_id, ai_type, data1,"
                        "data2, data3, data4) VALUES (?,?,?,?,?,?)",
                        (*DBSchemaName == '\0' ? "" : DBSchemaName),
                        table_names[TABLENAME_POS_ACCESSINFO]);
                err = SQLPrepare(statement2, CHAR2SQL(cmd), SQL_NTS);
                if (error_s(statement2, err, NULL) != 0) {
                    message('E', "SQLPrepare failed");
                    return E_ERROR;
                }

                err = SQLBindParameter(statement2,
                                       1,
                                       SQL_PARAM_INPUT,
                                       SQL_C_SLONG,
                                       SQL_INTEGER,
                                       0,
                                       0,
                                       &s_id,
                                       0,
                                       NULL);

                if (error_s(statement2, err, NULL)) {
                    message('E', "SQLBindParameter failed");
                    return E_ERROR;
                }

                err = SQLBindParameter(statement2,
                                       2,
                                       SQL_PARAM_INPUT,
                                       SQL_C_SLONG,
                                       SQL_INTEGER,
                                       0,
                                       0,
                                       &sr,
                                       0,
                                       NULL);

                if (error_s(statement2, err, NULL)) {
                    message('E', "SQLBindParameter failed");
                    return E_ERROR;
                }


                for (j = 0; j < 2; j++) {
                    /* bind to correct variable type */
                    err = SQLBindParameter(statement2,
                                           (SQLUSMALLINT)(j+2+1),
                                           SQL_PARAM_INPUT,
                                           (SQLSMALLINT)
                                           getValueType(
                                                   access_info_fields[j+2]),
                                           (SQLSMALLINT)
                                           getParamType(
                                                   access_info_fields[j+2]),
                                           getColumnSize(
                                                   access_info_fields[j+2]),
                                           0,
                                           &param_value_accessinfo[j],
                                           0,
                                           NULL);

                    if (error_s(statement2, err, NULL)) {
                        message('E', "SQLBindParameter failed");
                        return E_ERROR;
                    }

                    err = SQLBindParameter(statement2,
                                           5,
                                           SQL_PARAM_INPUT,
                                           getValueType(access_info_fields[4]),
                                           getParamType(access_info_fields[4]),
                                           getColumnSize(access_info_fields[4]),
                                           0,
                                           param_value_accessinfo_data3,
                                           0,
                                           NULL);

                    if (error_s(statement2, err, NULL)) {
                        message('E', "SQLBindParameter failed");
                        return E_ERROR;
                    }

                    err = SQLBindParameter(statement2,
                                           6,
                                           SQL_PARAM_INPUT,
                                           getValueType(access_info_fields[5]),
                                           getParamType(access_info_fields[5]),
                                           getColumnSize(access_info_fields[5]),
                                           0,
                                           param_value_accessinfo_data4,
                                           0,
                                           NULL);

                    if (error_s(statement2, err, NULL)) {
                        message('E', "SQLBindParameter failed");
                        return E_ERROR;
                    }
                }

                /* --- insert special_facility record --- */
                err = SQLAllocHandle(SQL_HANDLE_STMT, testdb, &statement3);
                if (error_c(testdb, err) != 0) {
                    message('E', "SQLAllocHandle failed");
                    return E_ERROR;
                }

                sprintf(cmd, "INSERT INTO %s%s (s_id, sf_type,"
                        "is_active, error_cntrl, data_a, data_b) "
                        "VALUES (?,?,?,?,?,?)",
                        (*DBSchemaName == '\0' ? "" : DBSchemaName),
                        table_names[TABLENAME_POS_SPECIALFACILITY]);
                err = SQLPrepare(statement3, CHAR2SQL(cmd), SQL_NTS);
                if (error_s(statement3, err, NULL) != 0) {
                    message('E', "SQLPrepare failed");
                    return E_ERROR;
                }

                err = SQLBindParameter(statement3,
                                       1,
                                       SQL_PARAM_INPUT,
                                       SQL_C_SLONG,
                                       SQL_INTEGER,
                                       0,
                                       0,
                                       &s_id,
                                       0,
                                       NULL);

                if (error_s(statement3, err, NULL)) {
                    message('E', "SQLBindParameter failed");
                    return E_ERROR;
                }
                err = SQLBindParameter(statement3,
                                       2,
                                       SQL_PARAM_INPUT,
                                       SQL_C_SLONG,
                                       SQL_INTEGER,
                                       0,
                                       0,
                                       &sr,
                                       0,
                                       NULL);

                if (error_s(statement3, err, NULL)) {
                    message('E', "SQLBindParameter failed");
                    return E_ERROR;
                }

                for (j = 0; j < 3; j++) {
                    /* bind to correct variable type */
                    err = SQLBindParameter(statement3,
                                           (SQLUSMALLINT)(j+2+1),
                                           SQL_PARAM_INPUT,
                                           (SQLSMALLINT)
                                           getValueType(
                                                   special_facility_fields[
                                                           j+2]),
                                           (SQLSMALLINT)
                                           getParamType(
                                                   special_facility_fields[
                                                           j+2]),
                                           getColumnSize(
                                                   special_facility_fields[
                                                           j+2]),
                                           0,
                                           &param_value_special[j],
                                           0,
                                           NULL);

                    if (error_s(statement3, err, NULL)) {
                        message('E', "SQLBindParameter failed");
                        return E_ERROR;
                    }
                }

                err = SQLBindParameter(statement3,
                                       6,
                                       SQL_PARAM_INPUT,
                                       SQL_C_CHAR,
                                       SQL_CHAR,
                                       AI_DATA4_LENGTH,
                                       0,
                                       param_value_special_str,
                                       0,
                                       NULL);

                if (error_s(statement3, err, NULL)) {
                    message('E', "SQLBindParameter failed");
                    return E_ERROR;
                }

                /* --- insert call_forwarding record --- */
                err = SQLAllocHandle(SQL_HANDLE_STMT, testdb, &statement4);
                if (error_c(testdb, err) != 0) {
                    message('E', "SQLAllocHandle failed");
                    return E_ERROR;
                }

                sprintf(cmd, "INSERT INTO %s%s (s_id, sf_type, "
                        "start_time, end_time, numberx) VALUES (?,?,?,?,?)",
                        (*DBSchemaName == '\0' ? "" : DBSchemaName),
                        table_names[TABLENAME_POS_CALLFORWARDING]);
                err = SQLPrepare(statement4, CHAR2SQL(cmd), SQL_NTS);
                if (error_s(statement4, err, NULL) != 0) {
                    message('E', "SQLPrepare failed");
                    return E_ERROR;
                }

                err = SQLBindParameter(statement4,
                                       1,
                                       SQL_PARAM_INPUT,
                                       SQL_C_SLONG,
                                       SQL_INTEGER,
                                       0,
                                       0,
                                       &s_id,
                                       0,
                                       NULL);

                if (error_s(statement4, err, NULL)) {
                    message('E', "SQLBindParameter failed");
                    return E_ERROR;
                }
                err = SQLBindParameter(statement4,
                                       2,
                                       SQL_PARAM_INPUT,
                                       SQL_C_SLONG,
                                       SQL_INTEGER,
                                       0,
                                       0,
                                       &sr,
                                       0,
                                       NULL);

                if (error_s(statement4, err, NULL)) {
                    message('E', "SQLBindParameter failed");
                    return E_ERROR;
                }

                for (x = 0; x < 2; x++) {
                    /* bind to correct variable type */

                    err = SQLBindParameter(statement4,
                                           (SQLUSMALLINT)(x+2+1),
                                           SQL_PARAM_INPUT,
                                           (SQLSMALLINT)
                                           getValueType(
                                                   call_forwarding_fields[x+2]),
                                           (SQLSMALLINT)
                                           getParamType(
                                                   call_forwarding_fields[x+2]),
                                           getColumnSize(
                                                   call_forwarding_fields[x+2]),
                                           0,
                                           &param_value_callfwd[x],
                                           0,
                                           NULL);

                    if (error_s(statement4, err, NULL)) {
                        message('E', "SQLBindParameter failed");
                        return E_ERROR;
                    }
                }

                param_length = SUBNBR_LENGTH;
                err = SQLBindParameter(statement4,
                                       5,
                                       SQL_PARAM_INPUT,
                                       SQL_C_CHAR,
                                       SQL_CHAR,
                                       param_length,
                                       0,
                                       param_value_callfwd_str,
                                       0,
                                       NULL);

                if (error_s(statement3, err, NULL)) {
                    message('E', "SQLBindParameter failed");
                    return E_ERROR;
                }

            } /* if (i == 0) */

            /* --- insert subscriber record --- */
            /* Get a random value for each column */
            for (j = 2; j < 34; j++) {
                param_value_subscriber[j] =
                    rnd(&rand, subscriber_fields[j], NULL);
            }

            /* Insert the record */
            err = SQLExecute(statement);
            ret = error_s(statement, err, "40001");
            if (ret) {
                message('E', "SQLExecute statement1 failed");                
                return E_ERROR;
            }
            if (err) {
                i = i - n_loops_after_commit;
                n_loops_after_commit = 0;
                continue;
            }
            
			/* --- insert access_info record --- */
			/* First figure out how many access_info records to insert (1-4) */
			subrecord_amount = get_random(&rand, 1, 4);
            count_access_info += subrecord_amount;
            
			for (j = 0; j < 4; j++) {
				sr_type[j] = 0;
			}

			for (k = 0; k < subrecord_amount; k++) {
				do {
					/* We insert 1-4 records and those records
                       inserted are actually inserted also in random
                       order (not like 1, 2 and then 3 (if three
                       records are inserted)) */
					sr = rnd(&rand, "ai_type", NULL);
				} while (sr_type[sr-1] == 1);
				sr_type[sr-1] = 1;

				/* Get the random values for each column */
				for (j = 0; j < 2; j++) {
					param_value_accessinfo[j] = rnd(&rand,
                                                    access_info_fields[j+2],
                                                    NULL);
				}

				rndstr(&rand, access_info_fields[4], subrstr);
				memcpy(param_value_accessinfo_data3, subrstr,
                       (AI_DATA3_LENGTH + 1) * sizeof(char));

				rndstr(&rand, access_info_fields[5], subrstr);
				memcpy(param_value_accessinfo_data4, subrstr,
                       (AI_DATA4_LENGTH + 1) * sizeof(char));

				/* Insert the record */
                err = SQLExecute(statement2);
                ret = error_s(statement2, err, "40001");
                if (ret) {
                    message('E', "SQLExecute statement2 failed");
                    return E_ERROR;
                } 
                if (err) {
                    break;
                }
                
			} /* END OF for (k=0 ... k < subrecord_amount *( */
            
            if (err) {
                i = i - n_loops_after_commit;
                n_loops_after_commit = 0;                
                continue;
            }
            
			/* --- insert special_facility record --- */
			/* First figure out how many special_facility records to
               insert (1-4) */
			subrecord_amount = get_random(&rand, 1, 4);
            count_special_facility += subrecord_amount;
            
			for (j = 0; j < 4; j++) {
				sf_type[j] = 0;
			}

			for (k = 0; k < subrecord_amount; k++) {
				do {
					/* We insert 1-4 records and those records inserted are
                       actually inserted also in random order
                       (not like 1, 2 and then 3
                       (if three records are inserted)) */
					sr = rnd(&rand, "sf_type", NULL);
				} while (sf_type[sr-1] == 1);
				sf_type[sr-1] = 1;

				/* Get the random values for each column */
				for (j = 0; j < 3; j++) {
					param_value_special[j] = rnd(&rand,
                                                 special_facility_fields[j+2],
                                                 NULL);
				}

				rndstr(&rand, special_facility_fields[5], subrstr);
				memcpy(param_value_special_str, subrstr,
                       (AI_DATA4_LENGTH + 1) * sizeof(char));

				/* Insert the record */
                err = SQLExecute(statement3);
                ret = error_s(statement3, err, "40001");
                if (ret) {
                    message('E', "SQLExecute statement3 failed");
                    return E_ERROR;
                }
                if (err) {
                    break;
                }

				/* --- insert call_forwarding record --- */
				/* First figure out how many special_facility records
                   to insert (0-3)*/
				callfwd_amount = get_random(&rand, 0, 3);
                count_call_forwarding += callfwd_amount;
                
				start_time = (int*)calloc(3, sizeof (int));

				for (j = 0; j < callfwd_amount; j++) {
					unsigned long number;
					/* Get the random values for each column */
					for (x = 0; x < 2; x++) {
						if (x == 1) {  /* end_time */
							param_value_callfwd[x] =
                                param_value_callfwd[x-1]
                                +(int)rnd(&rand, "end_time_add", NULL);
						} else if (x == 0) {
							do {
								st = rnd(&rand, call_forwarding_fields[x+2],
                                         NULL);
							} while (start_time[st/8] == 1);
							start_time[st/8] = 1;
							param_value_callfwd[x] = st;
						}
					}

                    /* min(s_id) and max(s_id) defined for client process */
					number = get_random(&rand, min_subs_id, max_subs_id);
					sub_nbr_gen(number, param_value_callfwd_str);

					/* Insert the record */
                    err = SQLExecute(statement4);
                    ret = error_s(statement4, err, "40001");
                    if (ret) {
                        message('E', "SQLExecute statement4 failed");
                        return E_ERROR;
                    }
                    if (err) {
                        break;
                    }
                    
				}
				free(start_time);
                if (err) {
                    break;
                }
			}
            if (err) {
                i = i - n_loops_after_commit;
                n_loops_after_commit = 0;
                continue;
            }

            /* commit, if not autocommit */
            if (commitblock > 0) {
                if (((i + 1) % commitblock == 0)
                    || ((i + 1) == subscribers_to_populate)) {
                    err = SQLEndTran(SQL_HANDLE_DBC, testdb, SQL_COMMIT);
                    if (error_c(testdb, err)) {
                        message('E', "Commit while populating failed.");
                        return E_ERROR;
                    }
                    n_loops_after_commit = 0;
                }
            }
            

        } /* END OF for (i=0; i < population size;i++) */

        free(s_ids);

        err = SQLFreeHandle(SQL_HANDLE_STMT, statement);
        err = SQLFreeHandle(SQL_HANDLE_STMT, statement2);
        err = SQLFreeHandle(SQL_HANDLE_STMT, statement3);
        err = SQLFreeHandle(SQL_HANDLE_STMT, statement4);
        
        sprintf(msg, "Inserted %d '%s' rows, %d '%s' rows (%.1fx), %d '%s' rows (%.1fx)"
                " and %d '%s' rows (%.1fx)",
                subscribers_to_populate, table_names[TABLENAME_POS_SUBSCRIBER],
                count_access_info, table_names[TABLENAME_POS_ACCESSINFO],
                (double)count_access_info/subscribers_to_populate,
                count_special_facility, table_names[TABLENAME_POS_SPECIALFACILITY],
                (double)count_special_facility/subscribers_to_populate,
                count_call_forwarding, table_names[TABLENAME_POS_CALLFORWARDING],
                (double)count_call_forwarding/subscribers_to_populate);
        message('D', msg);
        

        return 0;
}

/*##**********************************************************************\
 *
 *      populate
 *
 * Sets the population attributes and starts the
 * population by calling populateDatabase().
 *
 * Parameters:
 *      ConnectString 
 *          The database to connect to
 *
 *      DBSchemaFileName
 *			The name of the input file containing the
 *			schema definition (SQL) commands
 *
 *      DBSchemaName
 *          Database schema name
 *
 *      population_size
 *          Target database population size
 *
 *      populationCommitBlockRows
 *          Number of rows to insert between commits
 *          0 = autocommit
 *
 *      seq_order_keys
 *          0 = insert rows in subscriber table in random s_id order
 *          1 = insert rows in subscriber table in sequential s_id order
 *
 *      min_subscriber_id
 *          minimum S_ID value
 *
 *      max_subscriber_id
 *          maximum S_ID value
 *
 * Return value:
 *      0 - success 
 *     !0 - error
 */
int populate(char *connectinit_sql_file, SQLHDBC testdb, 
             char *DBSchemaName,
             int population_size, int populationCommitBlockRows,
             int seq_order_keys, int min_subscriber_id, int max_subscriber_id)
{
        int err = 0;
        char txt_buf[256];

        if (populationCommitBlockRows == 0) {
            /* Make sure that autocommit is on */
            err = SQLSetConnectAttr(testdb, SQL_ATTR_AUTOCOMMIT,
                                    (SQLPOINTER) SQL_AUTOCOMMIT_ON, 0);
            if (error_c(testdb, (RETCODE)err)) {
                message('E', "SQLSetConnectAttr failed");
                return E_ERROR;
            }
            message('D', "Autocommit mode is ON.");
        } else {
            /* Turn off the autocommit mode */
            err = SQLSetConnectAttr(testdb, SQL_ATTR_AUTOCOMMIT,
                                    (SQLPOINTER) SQL_AUTOCOMMIT_OFF, 0);
            if (error_c(testdb, (RETCODE)err)) {
                message('E', "SQLSetConnectAttr failed");
                return E_ERROR;
            }
            sprintf(txt_buf, "Population commit block size is %d.",
                    populationCommitBlockRows);
            message('D', txt_buf);
        }

        /* Populate the database */
        sprintf(txt_buf, "Populating %d subscribers "
                "(of total %d)",
                (max_subscriber_id - min_subscriber_id + 1),
                population_size);
        message('I', txt_buf);

        err = populateDatabase(testdb, population_size,
                               populationCommitBlockRows, seq_order_keys,
                               DBSchemaName, min_subscriber_id,
                               max_subscriber_id);
        if (err != 0) {
            message('E', "Error in the population phase");
            err = E_ERROR;
        }

        return err;
}
