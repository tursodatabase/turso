/**********************************************************************\
**  source      * timer.h
**  description * Timer control functions.
**
**                The timer is used the following way:
**		          1) define a variable of the type 'timertype_t'
**		          2) call 'initTimer' to initialize it.
**		          3) call 'startTimer' to start the timer (give 
**			         the variable of the type 'timertype_t' as 
**			         a parameter). Note: 'startTimer' actually 
**			         calls 'resetTimer' and 'restartTimer' (in 
**			         that order)
**		          4) get the time with the function 'readTimer'
**			         (give the variable of the type 'timertype_t'
**			         as a parameter)
**
**	It is also possible to stop the timer (with 'stopTimer')
**	and then read the time (with 'readTimer'). 
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public 
**  License 1.0 as published by the Open Source Initiative (OSI). 
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifndef TIMER_H
#define TIMER_H

#include <stdio.h>

#ifdef WIN32
#include <windows.h>
#include <windef.h>
#include <winbase.h>
#else
/* In case of other operating systems, use OS specifiec directives 
   (specified in the makefile) and an OS specific header */
#include "linuxspec.h"
/* the high resolution time (milliseconds etc.) definitions */
#include <time.h>
#endif

/* Timer scale */
enum {
    TIMER_MILLISECOND_SCALE = 1000,
    TIMER_MICROSECOND_SCALE = 1000000
};

/* Timertype_t type definition*/
struct timertype_t {
        int running;
        __int64 time;
        __int64 systemFrequency;  /* ticks per second */
        __int64 userFrequency;    /* ticks per second */
};

/* timer functions */
/* one-time timer initialization */
int initTimer(struct timertype_t *timer, __int64 scale);	
int resetTimer(struct timertype_t *timer);     
int startTimer(struct timertype_t *timer);      /* resets and restarts timer */
int stopTimer(struct timertype_t *timer);	
int readTimer(struct timertype_t *timer, __int64 *time); /* read timer value */

/* get current time */
int getTime();

/* The current system ticker / timeofday value is put in the reference 
   parameter tickerValue */
int getSystemTicker(__int64 *tickerValue);

int estimateTimerResolution(struct timertype_t *timer,double *timerRes);

#endif /* TIMER_H */
