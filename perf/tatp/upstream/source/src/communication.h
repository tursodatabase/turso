/**********************************************************************\
**  source       * communication.h
**  description  * Socket communication and protocol
**		           handling between modules. To initialize a 
**		           communication channel first call 
**		           initializeCommunication(..) and then either
**		           createConnection(..) or createListener(...) 
**		           to initialize a socket.
**		           Methods sendDataS(..) and receiveDataS(...) are
**		           provided for easy handling of TATP protocol
**		           messages.
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public 
**  License 1.0 as published by the Open Source Initiative (OSI). 
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifndef COMMUNICATION_H
#define COMMUNICATION_H

#ifdef WIN32
/* For the maximum number of sockets */
#define FD_SETSIZE 1024
#include <winsock2.h>
#define SLEEP(msec) Sleep(msec)
#define ERRNO() WSAGetLastError()
#define socklen_t int
#else /* not WIN32 */
#include "linuxspec.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#define SOCKET int
#define SOCKET_ERROR -1
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#define SLEEP(msec) usleep(1000*msec)
#include <errno.h>
#define ERRNO() errno
#endif

#ifndef OLD_FD
#include "thd.h"
#endif

#include "const.h"

/* Sender ID's for Main Control and Statistics modules. Remote Controls 
   use number starting from -3 and counting down */
/* To be used with composeMessage(), decodeMessage(), sendDataS() and 
   receiveDataS(). Negative numbers are used to avoid collisions with 
   the IDs used by the Clients (-> Clients use numbers, starting from 1,
   unique for each Client) */
#define REMOTE_CONTROL_ID_BASE -3
#define MAIN_CONTROL_ID -2
#define STATISTICS_ID -1

/* Listening port numbers for Control and Statistics modules */
#define CONTROL_PORT 2807
#define STATISTICS_PORT 2808
/* Clients 1-n are listening ports CLIENT_PORT_BASE + n-1 */
#define CLIENT_PORT_BASE 22002

#define MAIN_CONTROL_PORT CONTROL_PORT

/* Communication error codes */
#define COMM_ERR_INVALID_PROTOCOL -20141
#define COMM_ERR_CANNOT_SEND      -20142
#define COMM_ERR_UNKNOWN          -20143
#define COMM_ERR_LARGE_MESSAGE    -20144
#define COMM_ERR_NO_DATA          -20145
#define COMM_ERR_INVALID_TYPE     -20146
#define COMM_ERR_INITIALIZATION   -20147
#define COMM_ERR_OUT_OF_ORDER     -20148

/* Receive buffer size */
#define RX_BUFFER_SIZE 512000         /* Enough for multiple messages */
#define RX_BUFFER_ALERT_LIMIT 300000  /* If the buffer is used to this point 
                                         it is time to arrange more space.
                                         See the implementation of
                                         receiveMessage() */

/* Few important field sizes within a TATP protocol message */
#define TRANSACTIONTYPE_SIZE 128
#define ERRORTEXT_SIZE 256

/* Max number of connections which also means the maximum number of 
   client that can be used in a TATP benchmark */
#define MAX_CONNECTIONS 1024

#define DEFAULT_PROTO SOCK_STREAM /* TCP */

/* Message protocol frame start, end and separator tags. 
Lets keep the protocol simple to avoid big protocol overhead in 
the messages */
#define MSG_FRAME_START_1 '<'
#define MSG_FRAME_START_2 'S'
#define MSG_FRAME_START_3 '#'
#define MSG_FRAME_END_1 '#'
#define MSG_FRAME_END_2 'E'
#define MSG_FRAME_END_3 '>'
#define MSG_SEPARATOR ','

/* Higher level file transfer tags */
#define FILE_START_TAG "<TATP_INPUT_FILE>"
#define FILE_STOP_TAG "</TATP_INPUT_FILE>"

/* The message has "<S#nnn," at the beginning and "#E>" at the end 
   (over the message data) */
#define MESSAGE_PROTOCOL_OVERHEAD 10
/* The file has "<TATP_INPUT_FILE>" at first and "</TATP_INPUT_FILE>" in 
   the end thus the file protocol overhead is 35 */
#define FILE_PROTOCOL_OVERHEAD 35

/* Maximum message size */
#define MAX_MESSAGE_SIZE 512  /* If you change this, note that we limit the 
                                 message size given in the protocol to 3
                                 digits (at most 999 byte messages) */
/* Maximum size for a file we communicate over the socket interface */
#define MAX_FILE_SIZE 512000

/* different types of files that are sent from Main Control to the
    Remote Controls */
enum filetypes {
    UNDEFINED = -1,
    INIFILE = 0,
    TRANSACTIONFILE = 1,
    SCHEMAFILE = 2,
    LOGFILE = 3,
    OTHER = 4
};

#define NUM_FILETYPES 5

/* 'clientS' holds the status of a single client */
struct clientS
{
        int status;	                  /* client status: 
                                         0 = not connected or disconnected and 
                                         buffer read and cleared
                                         1 = connected and active
                                         2 = disconnected but data 
                                         remaining in the buffer */
        SOCKET sckClient;             /* receive socket */
        struct sockaddr_in clnt_addr; /* address of the client */
        char *buf_begin;              /* receive buffer begin pointer 
                                         (used as constant) */
        char *buf_read;               /* receive buffer read pointer */
        char *buf_write;              /* receive buffer write pointer */
        long bufferUsagePeak;         /* counts the maximum buffer usage for 
                                         monitoring the efficiency */
        long bufferMoves;             /* counts how many times buffer move was 
                                         performed (for monitoring) */
};

/* The message struct to be used with the TATP message protocol 
   encoding and decoding. Note that either 'reg', 'testparam', 
   'mqth', 'resptime' or 'file' or 'file' is actually used in a message
   instance.*/
struct message_dataS {
        unsigned int utime;           /* the "timestamp" */
        union {
                struct {              /* used with MSG_PING, MSG_REG, 
                                         MSG_LOGOUT, MSG_TIME and 
                                         MSG_COMPLETED type messages */
                        int testID;   /* Equals test_run_id from TIRDB 
                                         table TEST_RUNS */
                        char ip[W_L]; /* The IP address of the main 
                                         control. Used with the MSG_PING 
                                         messages */
                        int data;     /* The data field for the message. 
                                         This field carries different 
                                         type data depending on the 
                                         message type */
                } reg;
                struct {
                        /* used with the MSG_TESTPARAM type message */
                        char data[MAX_MESSAGE_SIZE];
                } testparam;
                struct {
                        /* used with the MSG_MQTH type message */
                        int timeSlotNum;
                        int transCount;
                } mqth;
                struct {
                        /* used with the MSG_RESPTIME type message */
#ifndef LINEAR_RESPONSE_SCALE
                        int slot;
                        int responseTimeBound;
#else
                        int responseTime;
#endif
                        char transactionType[TRANSACTIONTYPE_SIZE];
                        int transactionCount;
                } resptime;
                struct {
                        /* used with the MSG_FILE type message */
                        char fileFragment[MAX_MESSAGE_SIZE+1];
                } file;
        } sdata;
};

/* Message types to be used with composeMessage(), decodeMessage(),
   sendDataS() and receiveDataS() */
enum messageTypes {
	MSG_OK, 
	MSG_PING, 
	MSG_INTR, 
	MSG_REG, 
	MSG_FILE,
	MSG_TESTPARAM,
	MSG_SPAWNCLIENTS,
	MSG_TIME,
	MSG_STARTTEST,
	MSG_MQTH,
	MSG_RESPTIME,
	MSG_COMPLETED, 
	MSG_LOGOUT,
	MSG_CLEAN,
	MSG_LOGREQUEST
};

typedef struct communication_t {
        SOCKET sckListener;		                /* socket for the listener */
        int clientsConnected;	                /* count of clients connected */
        struct clientS client[MAX_CONNECTIONS]; /* client data for all 
                                                   the clients */
        struct sockaddr_in serv_addr;           /* address of the server */
} communication_t;

/* Communication functions */
int initializeCommunicationGlobal();
int finalizeCommunicationGlobal();
int initializeCommunication(communication_t *, char *modulename);
SOCKET createConnection(char *server_name, unsigned short port);
int createListener(communication_t *, unsigned short port);
int receiveMessage(communication_t *, char *message);
int sendMessage(SOCKET sck, char *message);
int disconnectConnection(communication_t *, SOCKET sck);
int finalizeCommunication(communication_t *);

/* Messaging functions */
int initializeMessaging();
int composeMessage(char *message, int senderID, int messageType, 
                   struct message_dataS *data);
int decodeMessage(char *message, int *senderID,	int *messageType, 
                  struct message_dataS *data);

/* Combined functions for easy handling of TATP protocol messages */
int sendDataS(SOCKET sck, int senderID, int messageType, 
              struct message_dataS *data);
int sendStartTestData(SOCKET sck, int senderID, 
                      struct message_dataS *data);
int receiveDataS(communication_t *, int *senderID, int *messageType, 
                 struct message_dataS *data);
#ifndef OLD_FD
int receiveDataS_mutexed(communication_t *, int *senderID, int *messageType, 
                         struct message_dataS *data, thd_mutex_t* mutex);
#endif


/* Send a file over network */
int sendFileToSocket(SOCKET sck, int senderId, char* fileName,
                     enum filetypes fileType);

/* Internally used auxiliar functions */
void portable_closesocket(SOCKET *sck);
void clearBuffers(communication_t *, int i);
int checkConnections(communication_t *);
int getMessage(communication_t *, int s);
int checkMessage(communication_t *);
int isMsgFrameStart(char *buf);
int isMsgFrameEnd(char *buf, int buf_length);
void writeLogSocketError(char *message);

#endif /* COMMUNICATION_H */
