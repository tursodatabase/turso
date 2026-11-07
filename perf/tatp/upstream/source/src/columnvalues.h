/**********************************************************************\
**  source       * columnvalues.h
**  description  * Functions for setting the TATP 
**		           benchmark table column values. Functions return 
**		           column values for TATP tables according to TATP
**		           specific rules.
**		           These fUnctions are used in both the target 
**		           database population phase (in the Control module) 
**		           and the transaction generation process (in Clients).
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public 
**  License 1.0 as published by the Open Source Initiative (OSI). 
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifndef COLUMNVALUES_H
#define COLUMNVALUES_H

#include <limits.h>
#include <string.h>
#ifdef WIN32
/* Windows header files */
#include <windows.h>
#else
/* UNIX header files */
#include <unistd.h>      
#include <stdlib.h>
#endif
#include "const.h"
#include "util.h"
#include <ctype.h>
#include <stdio.h>

struct rand_t;

/* Random functions */
long rnd (struct rand_t *, const char *param, char *string);
long rndstr (struct rand_t *, const char *param, char *string);

/* functions that return or check the type of given field (column) name */
int getValueType(char type[]);
int getParamType(char type[]);
int checkColumnType (char type[], int found_type);
int getColumnSize (char column_name[]);

/* converts s_id to string */
int sub_nbr_gen(long s_id, char* subnbr);

#endif /* COLUMNVALUES_H */
