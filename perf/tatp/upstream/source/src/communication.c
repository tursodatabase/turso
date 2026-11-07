/**********************************************************************\
**  source       * communication.c
**  description  * Socket communication and
**                 protocol handling between modules.
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#include <stdio.h>          /* for sprintf */
#include "communication.h"
#include "util.h"           /* for writeLog */
#ifndef OLD_FD
#include <sys/poll.h>
#include "thd.h"
#endif

#define MAX_CONN_ATTEMPTS 5

/*##**********************************************************************\
 *
 *      initializeCommunicationGlobal
 *
 * Initializes the communication environment
 *
 * Parameters:
 *      none
 * 
 * Return value:
 *      0  - success
 *     !0  - error
 *
 * Limitations:
 *      Meaningful only in Windows
 */
int initializeCommunicationGlobal( )
{

#ifdef WIN32
        WSADATA wsaData;
        
        /* Initilize WinSock */
        if (WSAStartup(MAKEWORD(1, 0), &wsaData)) {
            WSACleanup();
            writeLogSocketError("Initializing of WinSock failed. WSAStartup()");
            return COMM_ERR_INITIALIZATION;
        }
#endif

        return 0;
}

/*##**********************************************************************\
 *
 *      initializeCommunication
 *
 * Initialize the communication module. This should be called
 * first when starting to use the communication module.
 *
 * Parameters:
 *      comm 
 *          pointer to struct that holds communication data
 *
 *      modulename 
 *          Name of the calling module
 *
 * Return value:
 *      0 - success
 */
int initializeCommunication(communication_t *comm, char *modulename)
{        
        int i;
        struct clientS* client = comm->client;
        
        writeLog('D', "Initializing the communication module.");
        /* Clear all module globals */
        for (i = 0; i < MAX_CONNECTIONS; i++)
        {
            client[i].status = 0;
            client[i].buf_begin = NULL;
            client[i].buf_read = NULL;
            client[i].buf_write = NULL;
            client[i].sckClient = 0;
            client[i].bufferUsagePeak = 0;
            client[i].bufferMoves = 0;
        }
        comm->sckListener = 0;
        comm->clientsConnected = 0;
        
        return 0;
}

/*##**********************************************************************\
 *
 *      createConnection
 *
 * Create a socket and connect to the server.
 *
 * Parameters:
 *      server_name 
 *          Name or ip address of the server.
 *
 *      port
 *          Listener port of the service.
 *
 * Return value:
 *      0 - connection not opened
 *      connection handle - otherwise
 */
SOCKET createConnection(char *server_name, unsigned short port)
{
        struct sockaddr_in server;
        struct hostent *hp;
        unsigned int addr;
        int socket_type = DEFAULT_PROTO;
        SOCKET conn_socket;
        char msg[256];
        int connectAttempts;

        /* Try with IP-addr first */
        /* Convert nnn.nnn address to a usable one */

        addr = inet_addr(server_name);
        hp = gethostbyaddr((char *)&addr, 4, AF_INET);

        /* if hp == null, then server address might be a DNS name */
        if (hp == NULL) {
            hp = gethostbyname(server_name);
        }

        if (hp == NULL ) {
            sprintf(msg,"Cannot resolve address [%s]: Error %d.",
                    server_name, ERRNO());
            writeLog('E', msg);
            return 0;
        }

        /* Copy the resolved server address information into the
           sockaddr_in structure */
        memset(&server,0,sizeof(server));
        memcpy(&(server.sin_addr),hp->h_addr,hp->h_length);
        server.sin_family = hp->h_addrtype;
        server.sin_port = htons(port);

        /* Open a socket */
        conn_socket = socket(AF_INET,socket_type,0);
        set_FD_cloexec_flag((int)conn_socket, 1);

        if (conn_socket <= 0 ) {
            /* Could not open a socket */
            writeLogSocketError("socket()");
            return 0;
        }

        connectAttempts = 0;
        while (connect(conn_socket,(struct sockaddr *) &server,sizeof(server))
               == SOCKET_ERROR) {
            SLEEP(10);
			connectAttempts++;
			portable_closesocket(&conn_socket);
			if (connectAttempts > MAX_CONN_ATTEMPTS) {
				/* Could not connect */
				writeLogSocketError("connect()");
				return 0;
			}
			/* Open a new socket */
			conn_socket = socket(AF_INET,socket_type,0);
			set_FD_cloexec_flag((int)conn_socket, 1);
			if (conn_socket <= 0 ) {
				/* Could not open a socket */
				writeLogSocketError("socket()");
				return 0;
			}
		}

		/* The socket connection succeeded -> return the socket handle */
		return conn_socket;
}

/*##**********************************************************************\
 *
 *      createListener
 *
 * Create a listener socket.
  *
 * Parameters:
 *      comm 
 *          pointer to struct that holds communication data
 *
 *      port
 *          Port number for the service.
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int createListener(communication_t *comm, unsigned short port)
{
        SOCKET sckListener;
        struct sockaddr_in serv_addr;
        struct clientS* client = comm->client;
        int i;
        int optVal = 1;

        /* Set up the address structure */
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port = htons(port);

        /* Create a socket */
        sckListener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

        /* this is to avoid socket error 98 (because of TIME_WAIT state)
           when program is restarted */
        setsockopt(sckListener, SOL_SOCKET, SO_REUSEADDR, (char *)&optVal,
                   (int)sizeof(optVal));

        /* Bind the socket to the port */
        if (bind(sckListener, (struct sockaddr *)&serv_addr,
                 sizeof(serv_addr)) != 0) {
			/* an error occured */
			writeLogSocketError("createListener()");
			return -1;
        }

        set_FD_cloexec_flag((int)sckListener, 1);

        /* Listen at the binded socket for connections */
        if (listen(sckListener, SOMAXCONN) != 0) {
            /* an error occured */
            writeLogSocketError("createListener()");
            return -1;
        }

        /* Clear all clients */
        for (i = 0; i < MAX_CONNECTIONS; i++)
        {
            client[i].status = 0;
            client[i].buf_begin = NULL;
            client[i].buf_read = NULL;
            client[i].buf_write = NULL;
        }

        comm->clientsConnected = 0;
        comm->sckListener = sckListener;
        memcpy(&comm->serv_addr, &serv_addr, sizeof(serv_addr));

        return 0;
}

/*##**********************************************************************\
 *
 *      receiveMessage
 *
 * Checks for connections and incoming messages and receives
 * one message (if any). When selecting the message to
 * return from this method, we select the client whose
 * receive buffer has most data still to be handled and
 * return the oldest message from the buffer of that client.
 * 
 * Parameters:
 *      comm
 *          pointer to struct that holds communication data
 *
 *      message 
 *          Pointer to a buffer of a message received. Reserve
 *			at least MAX_MESSAGE_SIZE characters for the buffer.
 *
 * Return value:
 *     >=0  - length of the received message
 *      <0  - error
 */
int receiveMessage(communication_t *comm, char *message)
{
        int ci;             /* index for the client to be checked for
                               complete message */
        int buflen;         /* length of the received buffer */
        int msglen;         /* length of one message in the buffer */
        int i;              /* index of the client in search loop */
        int maxbufSize;     /* max buffer size found in the search loop */
        int retval;
        struct clientS* client = comm->client;

        /* Check if any client wants to connect to the server.
           Note: the function always succees and thus the return value
           is not checked */
        retval = checkConnections(comm);

        /* Check if any messages are being sent from the clients.
           If a message (fragment) is received it is put in the
           clients own message buffer. */
        retval = checkMessage(comm);
        if (retval < 0) {
            return retval;
        }

        /* The return value of this method: message length */
        msglen = 0;

        /* Find the client with most data waiting to be handled in
           the receive buffer. */
        ci = 0;
        maxbufSize = 0;
        for (i = 0; i < MAX_CONNECTIONS; i++) {
            if (client[i].status > 0) {
                /* Use either .buf_begin to find the most full buffer
                   or .buf_read to find most data in the buffer. */
                /* Both methods seem to work quite well. */
                buflen = client[i].buf_write - client[i].buf_begin;
                if (buflen > maxbufSize) {
                    maxbufSize = buflen;
                    ci = i;
                }
            }
        }
        if (client[ci].status > 0) {
            /* Check if there is a complete message in the receive buffer */
            buflen = client[ci].buf_write - client[ci].buf_read;
            if (buflen > MESSAGE_PROTOCOL_OVERHEAD) {
                /* Check the minimum requirements of the protocol for now */
                if (!isMsgFrameStart(client[ci].buf_read)) {
                    /* A protocol error: message tag(s) missing */
                    return COMM_ERR_INVALID_PROTOCOL;
                }
                /* Get the message length. First put a NULL (0) after
                   the starting message length field (replaces a ','
                   character). Restore the original character afterwards */
                client[ci].buf_read[6] = 0;
                msglen =
                    atoi(&(client[ci].buf_read[3])) + MESSAGE_PROTOCOL_OVERHEAD;
                client[ci].buf_read[6] = MSG_SEPARATOR;

                /* Check if the whole message is in the buffer */
                if (buflen >= msglen) {
                    /* Check that the message ends with a MSG_FRAME_END */
                    if (!isMsgFrameEnd(client[ci].buf_read, msglen)) {
                        /* A protocol error: message end tag missing  */
                        return COMM_ERR_INVALID_PROTOCOL;
                    }
                    /* Check that the message is not too large */
                    if (msglen > MAX_MESSAGE_SIZE) {
                        /* A protocol error: too long message */
                        return COMM_ERR_LARGE_MESSAGE;
                    }
                    strncpy(message, client[ci].buf_read, msglen);
                    client[ci].buf_read += msglen;
                    /* Check if the whole buffer is read */
                    if (buflen == msglen) {
                        if (client[ci].status == 2) {
                            /* Client was disconneced and now the buffer is
                               read completely. Free buffer and set status = 0,
                               meaning that the client is disconnected and
                               cleared. */
                            clearBuffers(comm, ci);
                        }
                        else {
                            /* Start from the beginning of the buffer */
                            client[ci].buf_read = client[ci].buf_begin;
                            client[ci].buf_write = client[ci].buf_begin;
                        }
                    }
                    else {
                        /* If the buffer is filling up, move the data from
                           buf_read to buf_begin. */
                        if (client[ci].buf_write - client[ci].buf_begin >=
                            RX_BUFFER_ALERT_LIMIT) {
							/* The buffer load is above the critical limit */
							buflen = client[ci].buf_write - client[ci].buf_read;
							if (client[ci].buf_read - client[ci].buf_begin >
                                buflen) {
								/* ... and there is enough room in the
                                   beginning of the buffer so move the data
                                   down in the beginning of the buffer */
								memmove(client[ci].buf_begin,
                                        client[ci].buf_read, buflen);
								/* write to the new place */
								client[ci].buf_write =
                                    client[ci].buf_begin + buflen;
								/* and read from the beginning. */
								client[ci].buf_read = client[ci].buf_begin;
								/* Increment the counter for monitoring
                                   purposes */
								client[ci].bufferMoves++;
							}
						}
                    }
                }
                else {
                    /* There were only a part of a message */
                    msglen = 0;
                }
            }
        }
        return msglen;
}

/*##**********************************************************************\
 *
 *      sendMessage
 *
 * Sends a message using a socket.
 *
 * Parameters:
 *      sck
 *          Socket to use.
 *
 *      message 
 *          Message buffer to send.
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int sendMessage(SOCKET sck, char *message)
{
        int len;
        int sent;
        char *buf;

        len = strlen(message);
        buf = message;
        while (len > 0) {
            sent = send(sck, buf, len, 0);
            if (sent == SOCKET_ERROR) {
                /*			writeLogSocketError("send()"); */
                return COMM_ERR_CANNOT_SEND;
            }
            /* sent may be less than len (in which case we need
               to send again) */
            len -= sent;
            buf += sent;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      disconnectConnection
 *
 * Disconnect the connection.
 *
 * Parameters:
 *      comm
 *          pointer to struct that holds communication data
 *
 *      sck 
 *          Socket to disconnect.
 *
 * Return value:
 *      0 - always
 */
int disconnectConnection(communication_t *comm, SOCKET sck)
{
        int i;
        int done = 0;
        struct clientS* client = comm->client;

        /* Find the socket to be closed */
        for (i = 0; i < MAX_CONNECTIONS; i++) {
            if (client[i].sckClient == sck) {
                /* Client found. Check if it is connected. */
                if (client[i].status == 1) {
                    if (sck) portable_closesocket(&sck);
                    comm->clientsConnected--;
                    done = 1;
                    if (client[i].buf_begin != NULL) {
                        /* Disconnected but possibly data in
                           the receive buffer */
                        client[i].status = 2;
                    }
                    else {
                        client[i].status = 0; /* Disconnected */
                    }
                }
                /* Check if buffers may be freed */
                clearBuffers(comm, i);
                break;
            }
        }
        /* It was not receive socket, it should be a send socket */
        if (!done) {
            if (sck) portable_closesocket(&sck);
            /* No need to do any cleaning in case of a send socket */
        }

        return 0;
}

/*##**********************************************************************\
 *
 *      finalizeCommunication
 * 
 * Finalize the Communication module
 *
 * Parameters:
 *      comm 
 *          pointer to struct that holds communication data
 *
 * Return value:
 *      0 - always
 */
int finalizeCommunication(communication_t *comm)
{
        int i;
        struct clientS* client = comm->client;

        if (comm->sckListener) {
            /* Disconnect the LISTENER socket. */
            portable_closesocket(&comm->sckListener);
        }

        /* To be sure, loop over all possible open connections
           and check their status */
        for (i = 0; i < MAX_CONNECTIONS; i++) {
            if (client[i].status > 0) {
                /* Make sure that the buffer will be freed in any case */
                client[i].buf_read = client[i].buf_write;
                disconnectConnection(comm, client[i].sckClient);
            }
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      finalizeCommunicationGlobal
 *
 * Clean up communication environment
 *
 * Parameters:
 *      none
 * 
 * Return value:
 *      0 - always
 *
 * Limitations:
 *      Meaningful only in Windows.
 */
int finalizeCommunicationGlobal( )
{         
#ifdef WIN32
         /* Remove Winsock */
         WSACleanup();
#endif         
        return 0;
}

/*##**********************************************************************\
 *
 *      initializeMessaging
 *
 * Initialize the messaging module.
 * (Nothing here so far, but call this for possible
 * future extensions.)
 *
 * Parameters:
 * Return value:
 *      0  - success
 *     !0  - error
 */
int initializeMessaging( )
{
        return 0;
}

/*##**********************************************************************\
 *
 *      composeMessage
 *
 * Compose a TATP protocol message from a data struct to
 * be put in the buffer
 * Few example messages:
 *	<S#007,-1,INTR#E>
 *	<S#005,-2,OK#E>
 *	<S#014,-1,REG,0,7,9,0#E>
 *	<S#019,-1,LOGOUT,1,15,12,0#E>
 *	<S#050,-1,TRANS,2,17,TRATYPE,122,2,W,This is a test only.#E>
 *
 * Parameters :
 *      message
 *          Pointer to a buffer of length MAX_MESSAGE_SIZE.
 *      senderID
 *          Id number of the sender module (either Control,
 *			Statistics or one of the clients).
 *      messageType
 *          Message type.
 *      data
 *          Struct of data to be composed (and returned to
 *			the caller).
 *
 * Return value:
 *     0  - success
 *    !0  - error
 */
int composeMessage(char *message, int senderID, int messageType,
                   struct message_dataS *data)
{
        static int messageNumber = 0;  /* Automatic message number */
        static char regName[] = "REG";
        static char logoutName[] = "LOGOUT";
        static char completedName[] = "COMPLETED";
        static char timeName[] = "TIME";

        int messageNumberIncrement;
        int msglen;     /* message length */
        char *msgdata;  /* pointer to the data part of the message */
        char *typeName; /* pointer to static names above */

        typeName = NULL;
        messageNumberIncrement = 0;
        msgdata = message + 7;  /* reserve space for frame start tag,
                                   message length and separator */

        /* Build the data part of the message (msgdata) */
        switch (messageType) {
            case MSG_MQTH:
                if (data == NULL) {
                    /* A MQTH type message needs to have data */
                    return COMM_ERR_NO_DATA;
                }
                /* Compose the message */
                sprintf(msgdata, "%d%cMQTH%c%d%c%d%c%d%c%d",
                        senderID, MSG_SEPARATOR, MSG_SEPARATOR,
                        messageNumber, MSG_SEPARATOR, data->utime,
                        MSG_SEPARATOR, data->sdata.mqth.timeSlotNum,
                        MSG_SEPARATOR, data->sdata.mqth.transCount);
                messageNumberIncrement = 1;
                break;
            case MSG_RESPTIME:
                if (data == NULL) {
                    /* A transaction type message needs to have data */
                    return COMM_ERR_NO_DATA;
                }
                /* Compose the message */
#ifndef LINEAR_RESPONSE_SCALE
                sprintf(msgdata, "%d%cRESPTIME%c%d%c%d%c%s%c%d%c%d%c%d",
                        senderID, MSG_SEPARATOR, MSG_SEPARATOR, messageNumber,
                        MSG_SEPARATOR, data->utime, MSG_SEPARATOR,
                        data->sdata.resptime.transactionType, MSG_SEPARATOR,
                        data->sdata.resptime.slot, MSG_SEPARATOR,
                        data->sdata.resptime.responseTimeBound, MSG_SEPARATOR,
                        data->sdata.resptime.transactionCount);
#else
                sprintf(msgdata, "%d%cRESPTIME%c%d%c%d%c%s%c%d%c%d",
                        senderID, MSG_SEPARATOR, MSG_SEPARATOR, messageNumber,
                        MSG_SEPARATOR, data->utime, MSG_SEPARATOR,
                        data->sdata.resptime.transactionType, MSG_SEPARATOR,
                        data->sdata.resptime.responseTime, MSG_SEPARATOR,
                        data->sdata.resptime.transactionCount);
#endif
                messageNumberIncrement = 1;
                break;
            case MSG_REG:
                if (!typeName) {
                    typeName = regName;
                }
            case MSG_LOGOUT:
                if (!typeName) {
                    typeName = logoutName;
                }
            case MSG_TIME:
                if (!typeName) {
                    typeName = timeName;
                }
            case MSG_COMPLETED:
                if (!typeName) {
                    typeName = completedName;
                }
                if (data == NULL) {
                    /* needs to have data */
                    return COMM_ERR_NO_DATA;
                }
                sprintf(msgdata, "%d%c%s%c%d%c%d%c%d%c%d",
                        senderID, MSG_SEPARATOR, typeName, MSG_SEPARATOR,
                        messageNumber, MSG_SEPARATOR, data->utime,
                        MSG_SEPARATOR, data->sdata.reg.testID, MSG_SEPARATOR,
                        data->sdata.reg.data);
                messageNumberIncrement = 1;
                break;
            case MSG_PING:
                if (data == NULL) {
                    /* A MSG_PING type message needs to have data */
                    return COMM_ERR_NO_DATA;
                }
                sprintf(msgdata, "%d%cPING%c%d%c%d%c%d%c%d%c%s",
                        senderID, MSG_SEPARATOR, MSG_SEPARATOR,
                        messageNumber, MSG_SEPARATOR, data->utime,
                        MSG_SEPARATOR, data->sdata.reg.testID, MSG_SEPARATOR,
                        data->sdata.reg.data, MSG_SEPARATOR,
                        data->sdata.reg.ip);
                messageNumberIncrement = 1;
                break;
            case MSG_TESTPARAM:
                if (data == NULL) {
                    /* A MSG_PING type message needs to have data */
                    return COMM_ERR_NO_DATA;
                }
                sprintf(msgdata, "%d%cTESTPARAM%c%d%c%d%c%s",
                        senderID, MSG_SEPARATOR, MSG_SEPARATOR,
                        messageNumber, MSG_SEPARATOR, data->utime,
                        MSG_SEPARATOR, data->sdata.testparam.data);
                messageNumberIncrement = 1;
                break;
            case MSG_OK:
                sprintf(msgdata, "%d%cOK", senderID, MSG_SEPARATOR);
                break;
            case MSG_INTR:
                sprintf(msgdata, "%d%cINTR", senderID, MSG_SEPARATOR);
                break;
            case MSG_STARTTEST:
                sprintf(msgdata, "%d%cSTARTTEST", senderID, MSG_SEPARATOR);
                break;
            case MSG_SPAWNCLIENTS:
                sprintf(msgdata, "%d%cSPAWNCLIENTS", senderID, MSG_SEPARATOR);
                break;
            case MSG_FILE:
                if (data == NULL) {
                    /* A file type message needs to have data */
                    return COMM_ERR_NO_DATA;
                }
                /* Compose the message */
                sprintf(msgdata, "%d%cFILE%c%d%c%s",
                        senderID, MSG_SEPARATOR, MSG_SEPARATOR,
                        messageNumber, MSG_SEPARATOR,
                        data->sdata.file.fileFragment);
                messageNumberIncrement = 1;
                break;
            case MSG_CLEAN:
                sprintf(msgdata, "%d%cCLEAN", senderID, MSG_SEPARATOR);
                break;
            case MSG_LOGREQUEST:
                sprintf(msgdata, "%d%cLOGREQUEST", senderID, MSG_SEPARATOR);
                break;
            default:
                /* Message type not recognized */
                return COMM_ERR_INVALID_TYPE;
        } /* switch (messageType) */

        /* Now, compose the whole message */
        msglen = strlen(msgdata);
        if (msglen >= MAX_MESSAGE_SIZE - MESSAGE_PROTOCOL_OVERHEAD) {
            /* Too long message. Should not actually happen. */
            return COMM_ERR_LARGE_MESSAGE;
        }
        message[0] = MSG_FRAME_START_1;
        message[1] = MSG_FRAME_START_2;
        message[2] = MSG_FRAME_START_3;
        /* Note: we reserve only 3 digits for the message length */
        sprintf(message + 3, "%03d", msglen);
        message[6] = MSG_SEPARATOR;
        msgdata[msglen] = MSG_FRAME_END_1;
        msgdata[msglen + 1] = MSG_FRAME_END_2;
        msgdata[msglen + 2] = MSG_FRAME_END_3;
        msgdata[msglen + 3] = 0;
        messageNumber += messageNumberIncrement;
        return 0;
}

/*##**********************************************************************\
 *
 *      decodeMessage
 *
 * Decode a TATP protocol message to a data struct given
 * as a parameter.
 *
 * Parameters:
 *      message
 *          Pointer to a message buffer to be decoded.
 *
 *      senderID
 *          Pointer to the id number of the sender (either
 *			Control, one of the remote controls, Statistics
 *          or one of the clients).
 *
 *      messageType
 *          Pointer to the message type.
 *
 *      data 
 *          Pointer to struct of data to hold the decoded message.
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int decodeMessage(char *message, int *senderID,	int *messageType,
                  struct message_dataS *data)
{
        /* Automatic message number verification */
        int messageNumber;
        int msglen;     /* message length */
        char *p, *p0;   /* pointers to some parts of the message */

        /* Check the minimum requirements of the protocol */
        if (!isMsgFrameStart(message)) {
            /* A protocol error: missing or invalid tags */
            return COMM_ERR_INVALID_PROTOCOL;
        }

        /* Get the message length. First put a NULL (0) after the starting
           message length field (replaces a ',' character). Restore the
           original character afterwards */
        message[6] = 0;
        msglen = atoi(&(message[3])) + MESSAGE_PROTOCOL_OVERHEAD;
        message[6] = MSG_SEPARATOR;

        /* Check that the message ends with a MSG_FRAME_END */
        if (!isMsgFrameEnd(message, msglen)) {
            /* A protocol error: missing end tag */
            return COMM_ERR_INVALID_PROTOCOL;
        }

        /* Get the sender ID */
        *senderID = atoi(&(message[7]));
        /* Get the message type */
        for (p = message + 7; *p != MSG_SEPARATOR; p++)
            ;
        p++;
        if (strncmp(p, "MQTH", 4) == 0) {
            *messageType = MSG_MQTH;
        }
        else if (strncmp(p, "RESPTIME", 8) == 0) {
            *messageType = MSG_RESPTIME;
        }
        else if (strncmp(p, "COMPLETED", 9) == 0) {
            *messageType = MSG_COMPLETED;
        }
        else if (strncmp(p, "LOGOUT", 6) == 0) {
            *messageType = MSG_LOGOUT;
        }
        else if (strncmp(p, "INTR", 4) == 0) {
            *messageType = MSG_INTR;
        }
        else if (strncmp(p, "STARTTEST", 9) == 0) {
            *messageType = MSG_STARTTEST;
        }
        else if (strncmp(p, "SPAWNCLIENTS", 12) == 0) {
            *messageType = MSG_SPAWNCLIENTS;
        }
        else if (strncmp(p, "REG", 3) == 0) {
            *messageType = MSG_REG;
        }
        else if (strncmp(p, "TIME", 4) == 0) {
            *messageType = MSG_TIME;
        }
        else if (strncmp(p, "PING", 4) == 0) {
            *messageType = MSG_PING;
        }
        else if (strncmp(p, "TESTPARAM", 9) == 0) {
            *messageType = MSG_TESTPARAM;
        }
        else if (strncmp(p, "OK", 2) == 0) {
            *messageType = MSG_OK;
        }
        else if (strncmp(p, "FILE", 4) == 0) {
            *messageType = MSG_FILE;
        }
        else if (strncmp(p, "CLEAN", 5) == 0) {
            *messageType = MSG_CLEAN;
        }
        else if (strncmp(p, "LOGREQUEST", 10) == 0) {
            *messageType = MSG_LOGREQUEST;
        }
        else {
            /* A protocol error: invalid message type */
            return COMM_ERR_INVALID_TYPE;
        }

        /* Decode the data part for the rest of the
           message types */
        for ( ; *p && *p != MSG_SEPARATOR; p++)
            ;
        p++;

        /* Now p points to the start of the data part
           of the message */
        switch (*messageType) {
            case MSG_MQTH:
                messageNumber = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* extract the "timestamp" from the message */
                data->utime = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* extract the time slot number  */
                data->sdata.mqth.timeSlotNum = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* ... and the count of transactions */
                data->sdata.mqth.transCount = atoi(p);
                break;
            case MSG_RESPTIME:
                messageNumber = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* the "timestamp" */
                data->utime = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                p0 = p;
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                /* Overwrite the separator */
                *p = 0;
                /* extract the transaction type */
                strcpy(data->sdata.resptime.transactionType, p0);
                /* put back the message separator */
                *p = MSG_SEPARATOR;
                p++;
#ifndef LINEAR_RESPONSE_SCALE
                /* extract the slot number from the message */
                data->sdata.resptime.slot = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* extract the response time slot from the message */
                data->sdata.resptime.responseTimeBound = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
#else
                data->sdata.resptime.responseTime = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
#endif
                /* extract the transaction count from the message */
                data->sdata.resptime.transactionCount = atoi(p);
                break;
            case MSG_REG:
            case MSG_LOGOUT:
            case MSG_TIME:
            case MSG_COMPLETED:
                messageNumber = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* extract the "timestamp" from the message */
                data->utime = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* extract the test identifier from the message */
                data->sdata.reg.testID = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* ... and finally the data */
                data->sdata.reg.data = atoi(p);
                break;
            case MSG_PING:
                messageNumber = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* extract the "timestamp" from the message */
                data->utime = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* extract the test identifier from the message */
                data->sdata.reg.testID = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* ... and the data */
                data->sdata.reg.data = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* ... and the ip address of the main control */
                /* Overwrite the first char of the frame end tag */
                message[msglen-3] = 0;
                /* extract the ip address from the message */
                strcpy(data->sdata.reg.ip, p);
                break;
            case MSG_TESTPARAM:
                messageNumber = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* extract the "timestamp" from the message */
                data->utime = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* ... and the parameter data */
                /* Overwrite the first char of the frame end tag */
                message[msglen-3] = 0;
                /* extract the data from the message */
                strcpy(data->sdata.testparam.data, p);
                break;
            case MSG_FILE:
                messageNumber = atoi(p);
                for ( ; *p != MSG_SEPARATOR; p++)
                    ;
                p++;
                /* Extract the data */
                /* First overwrite the first char of the frame end tag */
                message[msglen-3] = 0;
                /* extract file content fragment from the message */
                strcpy(data->sdata.file.fileFragment, p);
                break;
            case MSG_INTR:
            case MSG_STARTTEST:
            case MSG_SPAWNCLIENTS:
            case MSG_OK:
            case MSG_CLEAN:
            case MSG_LOGREQUEST:
                /* Nothing to do for these type of messages */
                break;
            default:
                /* A protocol error: invalid message type */
                return COMM_ERR_INVALID_TYPE;
        }

        return 0;
}

/*##**********************************************************************\
 *
 *      sendDataS
 *
 * Combined compose and send functions for easy use to send short messages.
 *
 * Parameters:
 *      sck 
 *          Socket to use.
 *
 *      senderID
 *          ID number of the sender
 *
 *      messageType
 *          Message type
 *
 *      data 
 *          Struct of data to be composed and sent.
 *
 * Return value:
 *      return value of the sendMessage() function
 */
int sendDataS(SOCKET sck, int senderID, int messageType,
              struct message_dataS *data)
{
        int retval;
        /* Message buffer */
        char msg_buf[MAX_MESSAGE_SIZE];
        retval = composeMessage(msg_buf, senderID, messageType, data);
        if (retval == 0) {
            retval = sendMessage(sck, msg_buf);
        }
        return retval;
}

/*##**********************************************************************\
 *
 *      receiveDataS
 *
 * Combined receive and decode functions for easy use.
 * NOTE: This one is blocking. If you need a non-blocking one,
 * use directly the two functions called from this function.
 *
 * Parameters:
 *      comm 
 *          pointer to struct that holds communication data
 *
 *      senderID
 *          pointer to the id number of the sender.
 *
 *      messageType
 *          pointer to the message type.
 *
 *      data 
 *          struct of data to hold the received and decoded message.
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int receiveDataS(communication_t *comm, int *senderID,	int *messageType,
                 struct message_dataS *data)
{
        int retval;
        /* Message buffer. Size from the header file */
        char buf[MAX_MESSAGE_SIZE];

        while (1) {            
            retval = receiveMessage(comm, buf);
            if (retval > 0) {
                retval = decodeMessage(buf, senderID, messageType, data);
                return retval;
            }
            else if (retval < 0) {
                return retval;
            }
            else {
                /* retval == 0 meaning that there was no message and no
                   error either */
                SLEEP(10); /* time for a nap */
            }
        }
}

#ifndef OLD_FD
int receiveDataS_mutexed(communication_t *comm, int *senderID,	int *messageType,
                         struct message_dataS *data, thd_mutex_t* comm_mutex)
{
        int retval;
        /* Message buffer. Size from the header file */
        char buf[MAX_MESSAGE_SIZE];
        
        while (1) {            
            thd_mutex_lock(comm_mutex);
            retval = receiveMessage(comm, buf);
            thd_mutex_unlock(comm_mutex);
            if (retval > 0) {
                retval = decodeMessage(buf, senderID, messageType, data);
                return retval;
            }
            else if (retval < 0) {
                return retval;
            }
            else {
                /* retval == 0 meaning that there was no message and no
                   error either */
                SLEEP(10); /* time for a nap */
            }
        }
}
#endif

/*##**********************************************************************\
 *
 *      sendFileToSocket
 *
 * Sends a file to a socket.
 *
 * Parameters:
 *      sck 
 *          socket to send to
 *
 *      senderID 
 *          ID of the sender
 *
 *      fileName
 *          The file to send
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int sendFileToSocket(SOCKET sck, int senderID, char* fileName,
    enum filetypes fileType)
{
        FILE *Fin;
        char msg[300]; /* TODO AUTOBUF */
#ifdef FILETYPE
        char buf[10];
#endif
        struct message_dataS data;
        int retval;

        /* Check first that the file name is not too long (we send it in
           one message) */
        if (strlen(fileName) > MAX_MESSAGE_SIZE
            - (MESSAGE_PROTOCOL_OVERHEAD+2)) {
            return -1; /* TODO CONST */
        }

        /* Read the file */
        Fin = fopen(fileName, "r"); /* TODO rt */
        if (!Fin) {
            sprintf(msg, "Cannot open file %s", fileName);
            message('F', msg);
            return E_FATAL; /* Fatal error */
        }

        /* Send first just the file start tag */
        strcpy(data.sdata.file.fileFragment, FILE_START_TAG);
        retval = sendDataS(sck, senderID, MSG_FILE, &data);
        if (retval != 0) {
            fclose(Fin);
            return retval;
        }

#ifdef FILETYPE        
        /* Add filetype */
        itoa(1, buf, 10);
        strcpy(data.sdata.file.fileFragment, buf);
        /* Add "," to separate the filetype from the following file content */
        strcat(data.sdata.file.fileFragment, ",");
        
        retval = sendDataS(sck, senderID, MSG_FILE, &data);
        if (retval != 0) {
            fclose(Fin);
            return retval;
        }
#endif
        
        /* Then send the file name */
        strcpy(data.sdata.file.fileFragment, fileName);
        /* Add "," to separate the file name from the following file content */
        strcat(data.sdata.file.fileFragment, ",");
        retval = sendDataS(sck, senderID, MSG_FILE, &data);
        if (retval != 0) {
            fclose(Fin);
            return retval;
        }

        /* Send the file content fragment by fragment where the size of
           one fragment is MAX_MSG_SIZE */
        for (;;) {

            char *contentPtr = data.sdata.file.fileFragment;
            /* Lets be on the safe side and send only MAX_MESSAGE_SIZE/2
               characters from the original message at a time. This leaves
               plenty of space for the protocol overhead */
            char *contentEnd = data.sdata.file.fileFragment
                + MAX_MESSAGE_SIZE / 2;

            while(fgets(contentPtr, contentEnd - contentPtr, Fin) != NULL) {
                if (strlen(contentPtr) == 0) {
                    break;
                }
                contentPtr += strlen(contentPtr);
            }

            if (contentPtr == data.sdata.file.fileFragment) {
                break;
            }

            *contentPtr = '\0';

            retval = sendDataS(sck, senderID, MSG_FILE, &data);
            if (retval != 0) {
                fclose(Fin);
                return retval;
            }
        }

        /* Finally, send just the file end tag */
        strcpy(data.sdata.file.fileFragment, FILE_STOP_TAG);
        retval = sendDataS(sck, senderID, MSG_FILE, &data);
        if (retval != 0) {
            fclose(Fin);
            return retval;
        }

        fclose(Fin);
        sprintf(msg, "The file %s sent over network", fileName);
        message('D', msg);
        return 0;
}

/*##**********************************************************************\
 *
 *      portable_closesocket
 *
 * Closes the socket. Does not return any error codes.
 *
 * Parameters:
 *      sck 
 *          Socket to close.
 *
 * Return value:
 *      none
 */
void portable_closesocket(SOCKET *sck)
{
#ifdef WIN32
        if (shutdown(*sck, SD_BOTH) != 0) {
            /* Ignore the errors */
        }
        if (closesocket(*sck) != 0) {
            /* Ignore the errors */
        }
#else
        if (shutdown(*sck, SHUT_RDWR) != 0) {
            /* Ignore the errors */
        }
        if (close(*sck) != 0) {
            /* Ignore the errors */
        }
#endif
        *sck = 0;
}

/*##**********************************************************************\
 *
 *      clearBuffers
 *
 * Clears the client buffers and changes status from 2 to 0.
 * Called by receiveMessage() and disconnectConnection()
 *
 * Parameters:
 *      comm
 *          pointer to struct that holds communication data
 *
 *      i 
 *          index of the client.
 *
 * Return value:
 *      none
 */
void clearBuffers(communication_t *comm, int i)
{
#ifdef _DEBUG
        char msg[256];
#endif
        struct clientS* client = comm->client;
        /* Checking if buffers may be freed */
        /* (status = 2 -> disconnected but data remaining in the buffer) */
        if (client[i].status == 2) {
            if (client[i].buf_read >= client[i].buf_write) {
                /* All data read */
                if (client[i].buf_begin != NULL) {
                    /* free the receive buffer */
                    free(client[i].buf_begin);
                }
                client[i].buf_begin = NULL;
                client[i].buf_read = NULL;
                client[i].buf_write = NULL;
                client[i].status = 0; /* Disconnected */
#ifdef _DEBUG
                /* Report interesting buffer usage. This is really
                   for software analysis only. */
                if (client[i].bufferUsagePeak > 5000 ||
                    client[i].bufferMoves > 0) {
					sprintf(msg, "Buffer %d usage: %ld, moves: %ld.", i,
                            client[i].bufferUsagePeak, client[i].bufferMoves);
					writeLog('I', msg);
				}
#endif
            }
        }
}

/*##**********************************************************************\
 *
 *      checkConnections
 *
 * Non-blocking check and accept of incoming socket
 * connections. If a client is connecting, a free spot
 * from the client structure is allocated for it.
 *
 * Parameters:
 *      comm
 *          pointer to struct that holds communication data
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int checkConnections(communication_t *comm)
{
        int s;
        int nfds;				/* For compatibility */

#ifndef OLD_FD
        struct pollfd fds[1];
#else
        fd_set conn;
#endif
         /* Length of the client address structure */
        socklen_t clientAddrLength;	
        int index;
        struct timeval timeout;
        struct clientS* client = comm->client;
        SOCKET sckListener = comm->sckListener;

        clientAddrLength = sizeof(client->clnt_addr);
        timeout.tv_sec = 0; /* 0 for non blocking */
        timeout.tv_usec = 0;

        if (comm->clientsConnected < MAX_CONNECTIONS) {
            /* Still room for new connections */
#ifndef OLD_FD
            memset(fds, 0, sizeof(fds));
            fds[0].fd = sckListener;
            fds[0].events = POLLIN;
#else
            FD_ZERO(&conn);
            /* Get the data from the listening socket */
            FD_SET(sckListener, &conn);
#endif
            /* Increment the nfds value by one, shouldnt be the same for
               each client that connects for compatibility reasons */
            nfds = sckListener + 1;
#ifndef OLD_FD
            s = poll(fds, 1, 0);
#else
            s = select(nfds, &conn, NULL, NULL, &timeout);
#endif
            if (s > 0) {
                /* Someone is trying to connect */
                /* Find free slot for the connection */
                for (index = 0; index < MAX_CONNECTIONS; index++) {
                    if (client[index].status == 0) {
                        /* Slot found, accept connection */
                        client[index].sckClient =
                            accept(sckListener,
                                   (struct sockaddr *)&client[index].clnt_addr,
                                   &clientAddrLength);
                        set_FD_cloexec_flag((int)client[index].sckClient, 1);
                        comm->clientsConnected++;
                        client[index].status = 1;
                        client[index].buf_begin =
                            (char*) malloc(RX_BUFFER_SIZE);
                        if (client[index].buf_begin == NULL) {
                            /* Memory allocation for the
                               receive buffer failed */
                            writeLogSocketError("checkConnections() "
                                                "(memory allocation failed)");
                            return -1;
                        }
                        client[index].buf_read = client[index].buf_begin;
                        client[index].buf_write = client[index].buf_begin;
                        client[index].bufferUsagePeak = 0;
                        client[index].bufferMoves = 0;
                        /* All done, no need to scan remaining slots */
                        break;
                    }
                }
            }
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      getMessage
 *
 * Append the message fragment from the client to the
 * receive buffer.
 *
 * Parameters:
 *      comm 
 *          pointer to struct that holds communication data
 *
 *      s 
 *          index of the client in client struct
 *
 * Return value:
 *     >0  - number of bytes received
 *      0  - disconnect received
 *     <0  - error
 */
int getMessage(communication_t *comm, int s)
{
        int retval;
        int buf_used; /* number of bytes used in the client buffer */
        struct clientS* client = comm->client;

        buf_used = client[s].buf_write - client[s].buf_begin;

        /* Receive data from the socket. The data is appended in
           the end of the clients message buffer */
        retval = recv(client[s].sckClient, client[s].buf_write,
                      RX_BUFFER_SIZE - buf_used, 0);

        if (retval > 0) {
            /* Data received */
            /* Advance the buffer write position by the number
               of bytes received */
            client[s].buf_write += retval;
            buf_used += retval;

            if (buf_used > client[s].bufferUsagePeak) {
                /* Store the receive buffer usage peak for monitoring
                   the buffer usage */
                client[s].bufferUsagePeak = buf_used;
            }
        }
        else if (retval == 0) {
            /* Normal disconnect received */
            writeLog('D', "Normal disconnect received.");
            disconnectConnection(comm, client[s].sckClient);
        }
        else {
            /* Receiving the data returned an error */
            writeLogSocketError("recv()");
        }
        return retval;
}

/*##**********************************************************************\
 *
 *      checkMessage
 *
 * Check if there is a message coming in. If a message is coming it is
 * read and put into a buffer with getMessage()
 *
 * Parameters:
 *      comm
 *          pointer to struct that holds communication data
 *
 * Return value:
 *     >=0  - bytes received
 *     <0  - error
 */
int checkMessage(communication_t *comm)
{
#ifndef OLD_FD
        struct pollfd fds[MAX_CONNECTIONS];
#else
        fd_set input_set, exc_set; /* Input and error sets
                                      for the select function */
#endif
        int s;
        int nfds; /* For compatibility */
        int i;
        struct timeval timeout;
        int retval;
        struct clientS* client = comm->client;

        if (comm->clientsConnected <= 0) {
            /* No clients connected */
            return 0;
        }

        retval = 0;
        timeout.tv_sec = 0;
        timeout.tv_usec = 0;
        nfds = 0;

#ifndef OLD_FD
        memset(fds, 0, sizeof(fds));
#else
        /* Set up the input and exception sets for select().*/
        FD_ZERO(&input_set);
        FD_ZERO(&exc_set);
#endif
        
        /* Input and error sets will look at all the sockets connected */
        for (i = 0; i < MAX_CONNECTIONS; i++) {
            if (client[i].status == 1) {
                /* The connection is active */
                /* poll only clients that have space in buffer */
                if (client[i].buf_begin + RX_BUFFER_SIZE
                    > client[i].buf_write) {
#ifndef OLD_FD
                    fds[i].fd = client[i].sckClient;
                    fds[i].events = POLLIN | POLLPRI;
#else
                    FD_SET(client[i].sckClient, &input_set);
#endif
                }

#ifdef OLD_FD
                FD_SET(client[i].sckClient, &exc_set);
#endif                
                if (nfds < (int)client[i].sckClient) {
                    nfds = (int)client[i].sckClient;
                }
            }
        }

#ifndef OLD_FD
        s = poll(fds, MAX_CONNECTIONS, 0);

        if (s < 0) {
          printf("error: %d\n", ERRNO());
        }
        /* TODO check poll() errors */
        
        if (s > 0) {
            /* Data coming in */
            for (i = 0; i < MAX_CONNECTIONS; i++) {
                /* Find a client giving error or data */
                if (client[i].status == 1) {
                    if ((fds[i].revents & POLLHUP) || (fds[i].revents & POLLERR)
                        || (fds[i].revents & POLLNVAL)) {

                        printf("error: %d\n", ERRNO());

                        /* Error */
                        disconnectConnection(comm, client[i].sckClient);
                        retval = COMM_ERR_UNKNOWN;
                    }
                    else if ((fds[i].revents & POLLIN)
                             || (fds[i].revents & POLLPRI)) {
                        //else {
                        /* Data */
                        /* Get the message (or possible just part of it) and
                           append it in the clients receive buffer */
                        retval = getMessage(comm, i);
                        if (retval < 0) {
                            /* Error encountered
                               -> stop iterating through the connections */
                            break;
                        }
                    }
                }
            }            
        }
        
#else        
        s = select(nfds+1, &input_set, NULL, &exc_set, &timeout);

        if (s > 0)  {
            /* Data coming in */
            for (i = 0; i < MAX_CONNECTIONS; i++) {
                /* Find a client giving error or data */
                if (client[i].status == 1) {
                    if (FD_ISSET(client[i].sckClient, &exc_set)) {
                        /* Error */
                        disconnectConnection(comm, client[i].sckClient);
                        retval = COMM_ERR_UNKNOWN;
                    }
                    else if (FD_ISSET(client[i].sckClient, &input_set)) {
                        /* Data */
                        /* Get the message (or possible just part of it) and
                           append it in the clients receive buffer */
                        retval = getMessage(comm, i);
                        if (retval < 0) {
                            /* Error encountered
                               -> stop iterating through the connections */
                            break;
                        }
                    }
                }
            }
        }
#endif
        
        return retval;
}

/*##**********************************************************************\
 *
 *      isMsgFrameStart
 *
 * Checks that the character string passed as a parameter starts
 * with the protocol message start tag.
 *
 * Parameters:
 *      buf 
 *          text string that should start with the start tag
 *			of the message protocol
 *
 * Return value:
 *      1 - start tag exists
 *	    0 - start tag missing
 */
int isMsgFrameStart(char *buf)
{
        if (buf[0] != MSG_FRAME_START_1 ||
            buf[1] != MSG_FRAME_START_2 ||
            buf[2] != MSG_FRAME_START_3) {
			return 0;
		}
		return 1;
}

/*##**********************************************\
 *
 *      isMsgFrameEnd
 *
 * Checks that the character string passed as a parameter has the
 * end tag of the message protocol
 *
 * Parameters :
 *    buf
 *          text string that should end with the endtag
 *			of the message protocol
 *
 * Return value:
 *     1 - end tag exists
 *	   0 - end tag missing
 */
int isMsgFrameEnd(char *buf, int buf_length)
{
        if (buf[buf_length-1] != MSG_FRAME_END_3 ||
            buf[buf_length-2] != MSG_FRAME_END_2 ||
            buf[buf_length-3] != MSG_FRAME_END_1) {
			return 0;
		}
		return 1;
}

/*##**********************************************************************\
 *
 *      writeLogSocketError
 *
 * Write the socket error message to the log. Calls finally
 * a log writing function defined in util.h
 *
 * Parameters:
 *      message
 *          Name of the function where error happened (and possible
 *			other error related text).
 *
 * Return value:
 *     none
 */
void writeLogSocketError(char *message)
{        
        char msg[256];

        /* ERRNO defined for different environments */
        sprintf(msg, "%s: socket error %d", message, ERRNO());
        /* writeLog is defined in util.h */
        writeLog('E', msg);
}
