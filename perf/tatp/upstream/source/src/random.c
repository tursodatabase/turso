/**********************************************************************\
**  source       * random.c
**  description  * Random number functions
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#include "random.h"

extern unsigned long genrand_int32(rand_t* );

/*##**********************************************************************\
 *
 *      get_random
 *
 * Returns a random number (uniform distribution)
 *
 * Parameters:
 *      r
 *          struct to be passed to random number generator
 *
 *      x
 *          range minimum value
 *
 *      y
 *          range maximum value
 *
 * Return value:
 *      random number
 */
unsigned long get_random(rand_t* r, unsigned long x, unsigned long y)
{
    unsigned long i;
    i = genrand_int32(r);
    if (y >= x) {
        i %= (y - x + 1);
    }
    return i + x;
}

/*##**********************************************************************\
 *
 *      get_nurand
 *
 * Return a random number (non-uniform distribution)
 *
 * Parameters:
 *      r
 *          struct to be passed to random number generator
 *
 *      a
 *          key distribution value A
 *
 *      x
 *          range minimum value
 *
 *      y
 *          range maximum value
 *
 * Return value:
 *      random number
 */

unsigned long get_nurand(rand_t* r, unsigned long a, unsigned long x,
                         unsigned long y)
{
	return ((get_random(r, 0, a) | (get_random(r, x, y))) % (y - x + 1)) + x;
}
