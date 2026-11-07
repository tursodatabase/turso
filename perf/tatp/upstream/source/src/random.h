/**********************************************************************\
**  source       * random.h
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

#ifndef RANDOM_H
#define RANDOM_H

#define RAND_N 624

typedef struct rand_t {
        unsigned long mt[RAND_N]; /* the array for the state vector  */
        int mti/*=RAND_N+1*/;     /* mti==N+1 means mt[N] is not initialized */
} rand_t;

/* random function initialization */
void init_genrand(
        rand_t* ,
        unsigned long s); 

/* random function */
unsigned long get_random(
        rand_t* ,
        unsigned long x,
        unsigned long y);

/* non-uniform random function */
unsigned long get_nurand(
        rand_t* ,
        unsigned long a, 
        unsigned long x, 
        unsigned long y);

#endif /* RANDOM_H */
