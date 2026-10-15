/**********************************************************************\
**  source       * solid_accelerator.c
**  description  * Server functions for SolidDB accelerator library
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#include "server.h"
#include "util.h"
#include "const.h"
#include <string.h>

#ifdef ACCELERATOR

#define ACCELERATOR_SLEEP_TIME 2  /* seconds to wait after accelerator
                                     startup/shutdown */
#include <stdlib.h>
#include <errno.h>

/* solidDB accelerator header file */
#include <sscapi.h>

#ifdef WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

struct server_t
{
    char* ssc_argv[4];
    SscServerT ssc_handle;
};

const char* const server_name = "solidDB Accelerator Library";

int startServer(struct server_t** p_handle) {
        return startServer_args(p_handle, NULL);
}

/*##**********************************************************************\
 *
 *      startServer
 *
 * Starts solidDB Accelerator server
 *
 * Parameters:
 *      p_handle
 *          Pointer to server handle that is returned
 * Return value:
 *      solidDB Server Control API (SSC) error code
 */
int startServer_args(struct server_t** p_handle, char *argv[])
{
        int rc = 0;
        struct server_t* handle;
        
        handle = (struct server_t*) malloc(sizeof(struct server_t));
        
        if (handle != NULL) {           
            handle->ssc_argv[0] = "TATP"; 
            rc = SSCStartServer(
                    1, handle->ssc_argv,
                    &handle->ssc_handle,
                    SSC_STATE_OPEN);            
            if (rc == SSC_NODATABASEFILE) {                                
                message('I', "Could not find solidDB database file, "
                        "creating a new database.");
                handle->ssc_argv[1] = (char*) malloc((strlen(DEFAULT_DBUSER_UID)+3)*sizeof(char));
                strcpy(handle->ssc_argv[1], "-U");
                strcat(handle->ssc_argv[1], DEFAULT_DBUSER_UID);
                handle->ssc_argv[2] = (char*) malloc((strlen(DEFAULT_DBUSER_PWD)+3)*sizeof(char));
                strcpy(handle->ssc_argv[2], "-P");
                strcat(handle->ssc_argv[2], DEFAULT_DBUSER_PWD);
                handle->ssc_argv[3] = (char*) malloc((strlen(DEFAULT_DBUSER_UID)+3)*sizeof(char));
                strcpy(handle->ssc_argv[3], "-C");
                strcat(handle->ssc_argv[3], DEFAULT_DBUSER_UID);
                rc = SSCStartServer(
                        4, handle->ssc_argv,
                        &handle->ssc_handle,
                        SSC_STATE_OPEN);

                free(handle->ssc_argv[1]);
                free(handle->ssc_argv[2]);
                free(handle->ssc_argv[3]);
            }
            if (rc == SSC_SUCCESS) {
                msSleep(ACCELERATOR_SLEEP_TIME*1000);
                rc = 0;
            } else if (rc == SSC_INVALID_LICENSE) {
                message('E', "Invalid or missing solidDB license file.");
            }
        } else {
            rc = ENOMEM;
        }
        if (rc == 0) {
            *p_handle = handle;
        }
        return rc;
}

/*##**********************************************************************\
 *
 *      stopServer
 *
 * Stop solidDB Accelerator server
 *
 * Parameters:
 *      handle 
 *          Server handle
 *
 * Return value:
 *      solidDB Server Control API (SSC) error code
 */
int stopServer(struct server_t* handle)
{
    SSCStopServer(handle->ssc_handle, 1);
    msSleep(ACCELERATOR_SLEEP_TIME*1000);
    free(handle);
    return 0;
}

#endif
