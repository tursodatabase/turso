/**********************************************************************\
**  source      * tatp.h
**  description * Place for DBMS-specific header includes and macros
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public 
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifndef TATP_H
#define TATP_H

#ifdef WIN32
#include <windows.h>
#endif

#ifndef SOLID_BUILD
#include <sql.h>
#ifndef DB2_BUILD
#include <sqlext.h>
#else
#include <sqlcli.h>
#include <sqlcli1.h>
#endif

#ifdef TC_COUNT
#define SQL_ATTR_TF_LEVEL           1317  /* Solid extension attribute */
#define SQL_ATTR_TC_PRIMARY         1318  /* Solid extension attribute */
#define SQL_ATTR_TC_SECONDARY       1319  /* Solid extension attribute */
#define SQL_ATTR_TF_WAITING         1320  /* Solid extension attribute */
#define SQL_ATTR_PA_LEVEL           1321  /* Solid extension attribute */
#define SQL_ATTR_TC_WORKLOAD_CONNECTION 1322 /* Solid extension attribute */   
#endif /* TC_COUNT */

#else /* SOLID_BUILD */       
#include <solidodbc3.h>
#endif /* SOLID_BUILD */

#endif /* TATP_H */
