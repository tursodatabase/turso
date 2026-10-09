/**********************************************************************\
**  source       * tirdb.h
**  description  * TIRDB handling functions for TATP
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public 
**  License 1.0 as published by the Open Source Initiative (OSI). 
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifndef TIRDB_H
#define TIRDB_H

#include "communication.h"
#include "control.h"
#include "util.h"
#include "const.h"

#define TEST_SESSION_INSERT "INSERT INTO test_sessions \
(session_id, session_name, start_date, start_time, stop_date, \
stop_time, author, db_name, db_version, hardware_id, os_name, \
os_version, throughput_resolution, config_id, config_name, \
software_version, comments) \
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

#define TEST_RUN_INSERT "INSERT INTO test_runs \
(test_run_id, session_id, test_name, start_date, start_time, \
stop_date, stop_time, test_completed, test_number, client_count, \
rampup_time, subscribers, mqth_avg) \
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

#define TRANSACTION_MIX_INSERT "INSERT INTO transaction_mixes \
(test_run_id,transaction_type,percentage) VALUES (?,?,?)"

#define CLIENT_DISTRIBUTION_INSERT "INSERT INTO database_client_distributions \
(test_run_id,remote_name,remote_ip, client_count) VALUES (?,?,?,?)"

#define TEST_SESSION_UPDATE "UPDATE test_sessions SET stop_date = ?,\
stop_time = ? WHERE session_id = ?"

#define TEST_RUN_UPDATE "UPDATE test_runs SET stop_date = ?, \
stop_time = ?, mqth_avg = ? WHERE test_run_id = ?"

/* Functions that access TIRDB */
int addToTIRDB(SQLHDBC tirdb, char *table_name, struct ddfs *ddf);
int checkTIRDB(char *ConnectString, struct ddfs *ddf, int addMissing);
int checkOneFromTIRDB(SQLHDBC tirdb, char *table_name, char *where_clause);
int initialize_tirdb_for_session(char *ConnectString, struct ddfs *ddf,
                                 struct tdfs *tdf);
int initialize_tirdb_for_benchmark(int mode, char *ConnectString,
                                   char *resultFileName, struct bmr *bmrs,
                                   int i, struct tdfs *tdf);
int finalize_tirdb_for_benchmark(int mode, char *ConnectString,
                                 char *resultFileName, struct bmr *bmrs);
int finalize_tirdb_for_session(char *ConnectString, struct tdfs *tdf);

int saveConfigurationFile(SQLHDBC tirdb, struct ddfs *ddf);

/* Functions that access TIRDB */
int checkIfConfigurationExists(SQLHDBC tirdb, struct ddfs *ddf,
                               int *new_configuration);


#endif  /* TIRDB_H */
