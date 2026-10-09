/**********************************************************************\
**  source       * linuxspec.c
**  description  * Linux-specific function implementations
**
**
**  Copyright (c) Solid Information Technology Ltd. 2004, 2008
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#include "linuxspec.h"

char* i2a(unsigned i, char *a, unsigned r)
{
        if (i/r > 0) a = i2a(i/r,a,r);
        *a = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"[i%r];
        return a+1;
}

/*##**********************************************************************\
 *
 *      itoa
 *
 * Converts integer value to char.
 *
 * Parameters:
 *      i - integer to be converted
 *
 *      a - pointer to buffer in which to return the result
 *
 *      r - the radix for the conversion
 *
 * Return value:
 *      pointer to buffer a
 *
 * Limitations:
 *      radix between 2 and 36 inclusive
 *      range errors on the radix defaults it to base10
 * 
 */

char* itoa(int i, char *a, int r)
{
        if ((r < 2) || (r > 36)) {
            r = 10;
        }
        
        if (i<0) {
            *a = '-';
            *i2a(-(unsigned)i,a+1,r) = 0;
        } else {
            *i2a(i,a,r) = 0;
        }
        return a;
}
