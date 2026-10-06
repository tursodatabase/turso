/**********************************************************************\
**  source       * columnvalues.c
**  description  * The columnvalues module implements the
**                 functions used to set column values for the columns
**                 of the tables used by TATP benchmark. The functions
**                 are used from both the 'control' and the 'client'
**                 modules.
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#include "columnvalues.h"
#include "random.h"
#include <assert.h>

/*##**********************************************************************\
 *
 *      rndstr
 *
 * Returns a random string. Value scale is controlled with
 * the parameter 'param'. This function process random
 * values, used during the population only.
 *
 * Parameters:
 *      r 
 *          pointer to rand_t structure
 *      param 
 *          string describing the random string type
 *      string
 *          pointer to string that the random string should be put in
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
long rndstr(struct rand_t *r, const char *param, char *string)
 {         
         int i;
         char c;
         
         /* valid type names are specified in client module
            specification */
         if (strncmp(param, "data3", 5) == 0) {
             for (i = 0; i < AI_DATA3_LENGTH; i++) {
                 c = (char)(get_random(r, 1, 25)+64);
                 string[i] = c;
             }
             string[AI_DATA3_LENGTH] = '\0';
         }
         else {
             if ((strncmp(param, "data4", 5) == 0)
                 || (strncmp(param, "data_b", 6) == 0)) {
                 for (i = 0; i < AI_DATA4_LENGTH; i++) {
                     c = (char)(get_random(r, 1, 25)+64);
                     string[i] = c;
                 }
                 string[AI_DATA4_LENGTH] = '\0';
             } else {
                 message('F', "Invalid rndstr argument.");
                 return E_FATAL;
             }
         }
         return 0;
}

/*##**********************************************************************\
 *
 *      rnd
 *
 * Returns a random number. Value scale is controlled with
 * the parameter 'param'. This function processes random
 * values, used during the population only.
 *
 * Parameters:
 *      r 
 *          pointer to rand_t structure
 *      param 
 *          string describing the random number type
 *      string
 *          dummy, should be NULL
 *
 * Return value:
 *      random number
 */
long rnd(struct rand_t *r, const char* param, char* string)
{
        unsigned long i;
        
        /* valid type names are specified in client module
           specification */
        if ((strncmp(param, "msc_location", 12) == 0)
            || (strncmp(param, "vlr_location", 12) == 0)) {
            return get_random(r, 1, 0);
        }
        else if (strncmp(param, "bit", 3) == 0) {
            return get_random(r, 0, 1);
        }
        else if (strncmp(param, "is_active", 9) == 0) {
            i = get_random(r, 0, 99);
            if (i <= 14) {
                return 0;
            }
            else {
                return 1;
            }
        }
        else if (strncmp(param, "hex", 3) == 0) {
            return get_random(r, 0, 15);
        }
        else if ((strncmp(param, "byte", 4) == 0)
                 || (strncmp(param, "data1", 5) == 0)
                 || (strncmp(param, "data_a", 6) == 0)
                 || (strncmp(param, "data2", 5) == 0)
                 || (strncmp(param, "error_cntrl", 11) == 0)) {
            return get_random(r, 0, 255);
        }
        else if (strncmp(param, "start_time", 10) == 0) {
            return get_random(r, 0, 2)*8;
        }
        else if (strncmp(param, "end_time", 8) == 0) {
            return get_random(r, 0, 2)*8 + get_random(r, 0, 7)+1;
        }
        else if (strncmp(param, "end_time_add", 12) == 0) {
            return get_random(r, 1, 8);
        }
        else if ((strncmp(param, "ai_type",7) == 0)
                 || (strncmp(param, "sf_type",7) == 0)) {
            return get_random(r, 1, 4);
        }
        else {
            assert(! "Invalid rnd argument.");
            return -1 * INT_MAX;
        }
}

/*##**********************************************************************\
 *
 *      getValueType
 *
 * Returns a C data type for given parameter
 * (field in the database structure)
 *
 * Note that we bind all numeric values to 'long' type
 * because we have allocated a long array as storage for all types
 * of values. Binding to a different (i.e. shorter) numeric type
 * will mess up the byte order in some environments)
 *
 * Parameters:
 *      type
 *          string containing field name
 *
 * Return value:
 *      integer value containing the correct C data type
 *		for the given field
 */
int getValueType (char type[])
{
        int i;
        
        /* convert to lowercase */
        for (i = 0 ; type[i] != '\0'; i++) {
            type[i] = tolower(type[i]);
        }
        if ((strncmp(type, "data3",5) == 0)
            || (strncmp(type, "data4",5) == 0)
            || (strncmp(type, "data_b",6) == 0)
            || (strncmp(type, "numberx",7) == 0)
            || (strncmp(type, "sub_nbr",7) == 0)) {
            /* string */
            return SQL_C_CHAR;
        }
        else {
            return SQL_C_SLONG;
        }
}

/*##**********************************************************************\
 *
 *      getParamType
 *
 * Returns a SQL data type for given parameter
 * (field in the database structure)
 *
 * Parameters:
 *      type
 *           string containing field name
 *
 * Return value:
 *		integer value containing the correct SQL data
 *		type for the field
 */
int getParamType (char type[])
{
        int i;
        
        /* convert to lowercase */
        for (i = 0 ; type[i] != '\0'; i++) {
            type[i] = tolower(type[i]);
        }
        
        if ((strncmp(type, "data3",5) == 0)
            || (strncmp(type, "data_b",6) == 0)
            || (strncmp(type, "data4",5) == 0)
            || (strncmp(type, "numberx",7) == 0)
            || (strncmp(type, "sub_nbr",7) == 0)) {
            /* string */
            return SQL_VARCHAR;
        } else {
            return SQL_INTEGER;
        }
}

/*##**********************************************************************\
 *
 *      checkColumnType
 *
 * Matches the TATP type coumn name to the SQL type.
 *
 * Parameters:
 *      type 
 *          string containing the column name
 *
 *      found_type
 *			the SQL type to compare to the TATP column type
 *
 * Return value:
 *	     0 if type was correct
 *       -1 if type check failed
 */
int checkColumnType (char type[], int found_type)
{
        int i;
        
        /* convert the given column name to lowercase */
        for (i = 0 ; type[i] != '\0'; i++) {
            type[i] = tolower(type[i]);
        }
        
        if ((strncmp(type, "bit",3) == 0)
            || (strncmp(type, "hex",3) == 0)
            || (strcmp(type, "is_active") == 0)
            || (strcmp(type, "start_time") == 0)
            || (strcmp(type, "end_time") == 0)) {
            if (!((found_type == SQL_TINYINT)
                  || (found_type == SQL_SMALLINT))) {
                return -1;
            }
        }
        else if (strcmp(&type[2], "_type") == 0) {
            if (!((found_type == SQL_TINYINT)
                  || (found_type == SQL_SMALLINT))) {
                return -1;
            }
        }
        else if (strcmp(type, "s_id") == 0) {
            if (found_type != SQL_INTEGER) {
                return -1;
            }
        }
        else if ((strncmp(type, "byte",4) == 0)
                 || (strcmp(type, "error_cntrl") == 0)) {
            if (found_type != SQL_SMALLINT) {
                return -1;
            }
        }
        else if ((strncmp(type, "data3",5) == 0)
                 || (strncmp(type, "data_b",6) == 0)
                 || (strncmp(type, "data4",5) == 0)) {
            if (found_type != SQL_CHAR) {
                return -1;
            }
        }
        else if (strncmp(type, "data",4) == 0) {
            if (found_type != SQL_SMALLINT) {
                return -1;
            }
        }
        else if ((strncmp(type, "numberx",7) == 0)
                 || (strncmp(type, "sub_nbr",7) == 0)) {
            if (found_type != SQL_VARCHAR) {
                return -1;
            }
        }
        else if (strcmp(&type[3], "_location") == 0) {
            if (found_type != SQL_INTEGER) {
                return -1;
            }
        }
        else {
            return -1;
        }
        /* Everything OK */
        return 0;
}

/*##**********************************************************************\
 *
 *      getColumnSize
 *
 * Returns the expected column size for given column
 *
 * Parameters:
 *      column_name
 *          string containing the column name
 *
 * Return value:
 *      column size, if found
 *      -1 if size for the given column is not known
 */
int getColumnSize (char column_name[])
{
        int i;
        
        /* convert the given column name to lowercase */
        for (i = 0 ; column_name[i] != '\0'; i++) {
            column_name[i] = tolower(column_name[i]);
        }
        
        if (strncmp(column_name, "data3", 5) == 0) {
            return AI_DATA3_LENGTH;
        }
        else if (strncmp(column_name, "data_b", 6) == 0) {
            return SF_DATAB_LENGTH;
        }
        else if (strncmp(column_name, "data4", 5) == 0) {
            return AI_DATA4_LENGTH;
        }
        else if (strncmp(column_name, "numberx", 7) == 0) {
            return SUBNBR_LENGTH;
        }
        else if (strncmp(column_name, "sub_nbr", 7) == 0) {
            return SUBNBR_LENGTH;
        }
        else {
            return 0;
        }
}

/*##**********************************************************************\
 *
 *      sub_nbr_gen
 *
 * Generates data suitable to sub_nbr field.
 *
 * Parameters:
 *      s_id
 *          subscriber id number
 *
 *      subnbr
 *          pointer to string that the sub_nbr should be copied to
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int sub_nbr_gen(long s_id, char* subnbr) {
        char *returnstr;
        char *buffer;
        int len, i;
        
        buffer = (char*)calloc(10, sizeof(char));
        returnstr = (char*)calloc(SUBNBR_LENGTH + 1, sizeof(char));
        if (buffer == NULL || returnstr == NULL) {
            message('F', "Dynamic memory allocation failed.");
            return E_FATAL;
        }
        
        /* convert to string */
        sprintf(buffer, "%ld", s_id);
        len = strlen(buffer);
        
        /* fill with preceding zeros */
        for (i = 0; i < SUBNBR_LENGTH; i++) {
            if (i < SUBNBR_LENGTH-len) {
                returnstr[i] = '0';
            } else {
                returnstr[i] = buffer[i-(SUBNBR_LENGTH-len)];
            }
        }
        
        returnstr[SUBNBR_LENGTH] = '\0';
        
        /* copy the string to given location */
        strncpy(subnbr, returnstr, SUBNBR_LENGTH + 1);
        free(returnstr);
        free(buffer);
        
        return 0;
}
