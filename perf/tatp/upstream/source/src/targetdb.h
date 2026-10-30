
/********************************************************************** \
**  source      * targetdb.h
**  description * Target DB handling functions
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public 
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifndef TARGETDB_H
#define TARGETDB_H

#include "random.h"

#include "communication.h"
#include "util.h"
#include "const.h"
#include "columnvalues.h"
#include "tatp.h"

/* S_ID range for a client process */
static int min_subs_id = 0;
static int max_subs_id = 0;

#define TABLENAME_POS_SUBSCRIBER 0
#define TABLENAME_POS_ACCESSINFO 1
#define TABLENAME_POS_SPECIALFACILITY 2
#define TABLENAME_POS_CALLFORWARDING 3

/* Functions that access the target database */
int initializeTargetDatabase(SQLHDBC testdb, char *DBSchemaFileName, 
                             char *DBSchemaName);
int emptyTATPTables(SQLHDBC testdb, char *DBSchemaName);
int checkTargetDatabase(cmd_type cmd, SQLHDBC testdb, 
                        int expected_size, char *DBSchemaName);
int checkTableSchema(SQLHDBC testdb, char *DBSchemaName);
int checkTargetDBpopulation(SQLHDBC testdb, int expected_size, 
                            char *DBSchemaName);

int checkTableDefinition(char* cmd, char* DBSchemaName);

/* Functions that access the target database */
int populate(char *connectinit_sql_file,
             SQLHDBC testdb, 
             char *DBSchemaName, int population_size, 
             int populationCommitBlockRows, int seq_order_keys, 
             int min_subscriber_id, int max_subscriber_id);

int populateDatabase(SQLHDBC testdb, int population_size,
                     int populationCommitBlockRows, int seq_order_keys,
                     char *DBSchemaName, int min_subscriber_id,
                     int max_subscriber_id);

#endif /* TARGETDB_H */
