/**********************************************************************\
**  source       * server_dummy.c
**  description  * Dummy server functions
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

#ifndef ACCELERATOR

#include <stdlib.h>
#include <errno.h>

struct server_t
{
    int dummy;
};

const char* const server_name = "";

/*##**********************************************************************\
 *
 *      startServer
 *
 * Dummy function that just allocates memory because server is not
 * actually started. See solid_accelerator.c for a working
 * implementation.
 *
 * Parameters:
 *      p_handle 
 *          double pointer to server_t handle
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int startServer(struct server_t** p_handle)
{        
        int rc = 0;
        struct server_t* handle;
        
        handle = (struct server_t*) malloc(sizeof(struct server_t));
        
        if (handle != NULL) {
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
 * Dummy function for stopping a server.
 *
 * Parameters:
 *      handle 
 *          pointer to server struct
 *
 * Return value:
 *      0 - success
 */
int stopServer(struct server_t* handle)
{
        free(handle);
        return 0;
}

#endif
