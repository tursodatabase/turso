#ifndef FILEOPER_H
#define FILEOPER_H

#define OVERWRITE 1
#define DO_NOT_OVERWRITE 0

extern communication_t g_comm;

/* Receives a file content from the communication port.
Stores the file in the main directory. */
int receiveFile(const char* dataFragment, const char* path,
                int overwrite, enum filetypes* fileType);

#endif
