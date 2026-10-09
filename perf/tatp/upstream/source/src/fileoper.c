/**********************************************************************\
**  source       * fileoper.c
**  description  * 
**                 
**
**  Copyright (c) Solid Information Technology Ltd. 2004, 2010
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#include "util.h"
#include "communication.h"
#include "fileoper.h"

/*#*********************************************************************** \
 *
 *      parseFileHeader
 *
 * Tries to parse incoming message to look for file
 * name.
 *
 * Parameters:
 *      fileContent 
 *          Source file header, possibly incomplete
 *
 *      target 
 *          Pointer to the buffer for file name
 *
 *      targetLen 
 *          Length of target, in characters
 *
 * Return value:
 *      Pointer to next character after the header if full header was parsed
 *      NULL - if more data needed
 */

static const char *parseFileHeader(const char *fileContent, char *target,
                                   int targetLen, enum filetypes* fileType)
{       
        const char *fileEnd = fileContent + strlen(fileContent);
        const char *p1, *p2, *p3, *p4;
        char buf[10];
        int length;

        /* Extract the file type (FILE_START_TAG is <TATP_INPUT_FILE>) */
        p1 = fileContent;
        while (p1 != fileEnd && *p1 != '>') {
            p1++;
        }
        if (p1 == fileEnd) {
            return NULL;
        }
        p1++;
        p2 = p1;
        while (p2 != fileEnd && *p2 != ',') {
            p2++;
        }
        if (p2 == fileEnd) {
            return NULL;
        }
        p3 = p2+1;
        /* Now the file type is between p1 and p2 */
        length = p2 - p1;
        memcpy(buf, p1, length);
        buf[length] = '\0';
        *fileType = atoi(buf);
        
        p2++;
        p3 = p2;
        while (p3 != fileEnd && *p3 != ',') {
            p3++;
        }
        if (p3 == fileEnd) {
            return NULL;
        }
        p4 = p3+1;
        /* Now the file name is between p2 and p3 */
        length = p3 - p2;        
        if (length > targetLen - 1) {
            length = targetLen - 1;
        }
        memcpy(target, p2, length);
        target[length] = '\0';

        return p4;
}

/*##**********************************************************************\
 *
 *      receiveFile
 *
 * Receives a file content from the communication port.
 * Stores the file in the given path
 * The protocol of the received file is
 * FILE_START_TAGfileName,fileContentFILE_STOP_TAG
 *
 * Parameters:
 *      dataFragment
 *		    initial data fragment of the file message
 *
 *      path 
 *          path to use for file
 *
 * Return value:
 *      0  - success
 *     !0  - error
 */
int receiveFile(const char* dataFragment, const char* path,
                int overwrite, enum filetypes* fileType)
{
        int retval;
        /* Sender ID of the received message */
        int senderID;
        /* Type of the received message. */
        int messageType;
        /* receive file, but do not write into file */ 
        int dummy_receive = 0;        
        /* Struct to hold the received data */
        struct message_dataS data;
        char fileContent[MAX_MESSAGE_SIZE * 2];
        char msg[W_L];
        char target[W_L] = "";
        FILE *fp = NULL;
        char *finish = NULL;

        /* Append the filename to the path */
        if (path != NULL) {
            strncat(target, path, W_L);
        }        
        /* Copy the first fragment of the file from the input parameter */
        strcpy(fileContent, dataFragment);
        for (;;) {            
            const char *p = fileContent;            
            if (fp == NULL) {
                /* get file name */
                /* and file type */                
                p = parseFileHeader(fileContent, target + strlen(target),
                                    W_L - strlen(target), fileType);
                
                if (p != NULL) {                    
                    if (!overwrite) {
                        /* test if file exists*/
                        fp = fopen(target, "r");
                        if (fp != NULL) {
                            /* file already exists */
                            dummy_receive = 1;
                            *fileType = UNDEFINED;
                            sprintf(msg, "The file '%s' already exists "
                                    "and won't be overwritten.", target);
                            writeLog('I', msg);
                        }
                    }                    
                    if (!dummy_receive) {
                        /* open file with overwrite possibility */
                        fp = fopen(target, "w");
                        
                        if (fp == NULL) {
                            sprintf(msg, "Could not open the file '%s' for "
                                    "writing.", target);
                            writeLog('E', msg);
                            return E_ERROR;
                        }
                    }
                }
            }            
            if (fp != NULL) {                
                finish = strstr(p, FILE_STOP_TAG);
                if (finish != NULL) {
                    *finish = '\0';
                }                
                if (!dummy_receive) {
                    /* Write the file content */
                    if (fputs(p, fp) < 0) {
                        sprintf(msg, "Could not write a character to the "
                                "file %s.", target);
                        writeLog('E', msg);
                        return E_ERROR;
                    }
                }                
                *fileContent = '\0';
            }            
            if (finish != NULL) {
                break;
            }            
            /* receive next part of the file */
            retval = receiveDataS(&g_comm, &senderID, &messageType, &data);
            if (retval == 0) {
                /* main and remote nodes can send files */
                if (!(senderID <= MAIN_CONTROL_ID)) {
                    sprintf(msg,
                            "Received a message from an unexpected sender '%d'",
                            senderID);
                    writeLog('E', msg);
                    return E_ERROR;
                }
                else {
                    if (messageType != MSG_FILE) {
                        writeLog('E', "Wrong message type received "
                                 "(MSG_FILE expected)");
                    }
                    else {
                        /* Append the message content in the file content */
                        strcat(fileContent, data.sdata.file.fileFragment);
                    }
                }
            }
            else {
                sprintf(msg,
                        "Error %d at receiveDataS() while waiting message.",
                        retval);
                writeLog('E', msg);
                return E_ERROR;
            }
        }        
        if (fp != NULL) {
            fclose(fp);
        }      
        return E_OK;
}
