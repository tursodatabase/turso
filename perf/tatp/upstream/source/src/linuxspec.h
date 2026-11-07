/**********************************************************************\
**  source       * linuxspec.h
**  description  * Linux-specific definitions
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public 
**  License 1.0 as published by the Open Source Initiative (OSI). 
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifndef LINUX_H
#define LINUX_H

#ifndef WIN32

#include <stdint.h>

#define __int64 int64_t

/*
 * The following two functions together make up an itoa()
 * implementation. Function i2a() is a 'private' function
 * called by the public itoa() function.
 */
char* i2a(unsigned i, char *a, unsigned r);
extern char* itoa(int i, char *a, int r);

#endif

#endif /* LINUX_H */
