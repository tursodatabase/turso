/**********************************************************************\
**  source      * server.h
**  description * Server functions for SolidDB accelerator library
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public 
**  License 1.0 as published by the Open Source Initiative (OSI). 
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifndef TATP_SERVER_H
#define TATP_SERVER_H

struct server_t;

int startServer(struct server_t** handle);

int startServer_args(struct server_t** handle, char *argv[]);

int stopServer(
    struct server_t* handle);

extern const char* const server_name;

#endif /* TATP_SERVER_H */
