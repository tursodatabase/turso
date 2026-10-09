/**********************************************************************\
**  source       * remcontrol.h
**  description  * The remote control module is, as the name 
**		           suggests, the 'remote control' of the TATP benchmark
**		           'remote control' is in responsible of
**		           - communicating with the main control
**		           - starting the clients in the 'remote' machine
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public 
**  License 1.0 as published by the Open Source Initiative (OSI). 
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifndef REMCONTROL_H
#define REMCONTROL_H

#include "control.h"

/* Socket connections for the clients (MAX_CONNECTIONS 
   equals max num of clients per remote */
extern SOCKET clientScks[MAX_CONNECTIONS+1];

/* Remote control machine related data */
struct remoteControl {
        char name[W_L];	       /* The name given to the remote machine */
        char ip[W];	           /* the IP address of the remote machine */
        int port;              /* remote control port number*/
        int remoteControlId;   /* The remote control id, starts 
                                  descending from -3 (main control id is -2) */
        char targetDBdsn[W_L]; /* Target database DSN in the remote machine */
        SOCKET sck;            /* The socket handle for the remote 
                                  machine connection */
        int defined;	       /* flag indicating if the slot is defined */
        short pingStatus;      /* A status flag indicating the ping result */
        short clientsUp;       /* A status flag indicating whether the 
                                  clients are started */
};

/* The remote machines structure. We store the information starting
   from index 1 (and not 0) */
extern struct remoteControl remControls[MAX_NUM_OF_REMOTE_COMPUTERS];

/* The 'remote control' main loop that runs forever */
void remoteControl();
/* Initializes the structure to hold remote machine information */
void InitRemotesStruct();
/* Resolves the parameters sent by the main control*/
void resolveTestParameters(struct message_dataS *data, 
                           struct clientStartParameters *csp);
/* Checks the validity of the parameters sent by the main control*/
int validTestParameters(struct clientStartParameters *csp);
/* Waits for an OK message from all the clients */
int getRemClientResponses(struct clientStartParameters *csp, 
                          int waitDatabaseStart);
/* Measures the client resposne times */
int timeClientResponses(struct clientStartParameters *csp, 
                        int *longestClientResponse,
                        int myControlID, int mainControlTime, 
                        struct timertype_t *remoteInternalTime);
/* Create socket connections to all the remote's clients */
int createClientConnections(int numOfClients, int firstClientNum);
/* Disconnects all the remote's client connections */
void disconnectClientConnections();
/* Sends the log files to main control */
int sendLogFiles(SOCKET sck, int senderID,
                 int firstClient, int numOfClients);

#endif /* REMCONTROL_H */
