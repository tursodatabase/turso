/**********************************************************************\
**  source       * timer.c
**  description  * Timer control functions.
**
**	     	       For the usage of a timer see timer.h
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#include "timer.h"
#ifndef WIN32
#include <sys/time.h>
#endif
#include <sys/timeb.h>

enum
{
    THOUSAND = 1000,
    MILLION = 1000000
};

/*##**********************************************\
 *
 *      readRunningTimer
 *
 * Parameters :
 *    timer
 *		the timer variable that carries e.g. the status
 *		of the timer (the timer "object")
 *
 * Return value:
 *     0  - success
 *    !0  - error
 */
static int readRunningTimer(
    struct timertype_t *timer,
    __int64 *timerVal) {

#ifdef WIN32

    LARGE_INTEGER ticker;
    if (!QueryPerformanceCounter(&ticker)) {
        return -1;
    }
    *timerVal = ticker.QuadPart;

#else /* not WIN32 */

    struct timeval timev;
    if (gettimeofday(&timev, NULL) != 0) {
        return -1;
    }
    *timerVal = (__int64) timev.tv_sec * MILLION + timev.tv_usec;

#endif

    return 0;
}

/*##**********************************************\
 *
 *      initTimer
 *
 * If a WIN32 platform is used the system ticker
 * parameter is set (the frequency of the high-resolution
 * performance counter).
 *
 * Parameters :
 *    timer
 *		the timer variable that carries e.g. the status
 *		of the timer (the timer "object")
 *
 * Return value:
 *     0  - success
 *    !0  - error
 */
int initTimer(
    struct timertype_t *timer,
    __int64 frequency) {

#ifdef WIN32

    LARGE_INTEGER freq;
    /* Get the frequency of the high-resolution
       performance counter */
    if (!QueryPerformanceFrequency(&freq))
        return -1;

    timer->systemFrequency = freq.QuadPart;

#else

  /* expect that we always have gettimeofday() available */
    timer->systemFrequency = MILLION;

#endif

    timer->userFrequency = frequency;

    return 0;
}

/*##**********************************************\
 *
 *      resetTimer
 *
 * Resets the timer. Basically means zeroing the values
 * of the timer parameter. If a WIN32 platform is used
 * also the system ticker parameter is set (the frequency
 * of the high-resolution performance counter).
 *
 * Parameters :
 *    timer
 *		the timer variable that carries e.g. the status
 *		of the timer (the timer "object")
 *
 * Return value:
 *     0  - success
 *    !0  - error
 */
int resetTimer(struct timertype_t *timer) {
	timer->time = 0;
	/* Set status to 'stopped' */
	timer->running = 0;
	return 0;
}

/*##**********************************************\
 *
 *      startTimer
 *
 * Resets and restarts the timer.
 *
 * Parameters :
 *    timer
 *		the timer variable that carries e.g. the status
 *		of the timer (the timer "object")
 *
 * Return value:
 *     0  - success
 *    !0  - error
 */
int startTimer(struct timertype_t *timer) {

    timer->running = 1;
    readRunningTimer(timer, &timer->time);
    return 0;
}


/*##**********************************************\
 *
 *      stopTimer
 *
 * Stops the timer (use 'readTimer' ot read the timer
 * value).
 *
 * Parameters :
 *    timer
 *		the timer variable that carries e.g. the status
 *		of the timer (the timer "object")
 *
 * Return value:
 *     0  - success
 *    !0  - error
 */
int stopTimer(struct timertype_t *timer) {

    __int64 time;

    if (timer->running == 0) {
        /* Cannot stop the timer if it is not running */
        return -1;
    }

    readRunningTimer(timer, &time);
    timer->running = 0;
    timer->time = time - timer->time;

    if (timer->time < 0) {
        /* We should not come to this... */
        timer->time = 0;
    }
    return 0;
}


/*##**********************************************\
 *
 *      readTimer
 *
 * Reads the timer value. Note that we have different
 * implementation depending whether the timer status
 * is 'running' or 'stopped'.
 *
 * Parameters :
 *    timer
 *		the timer variable that carries e.g. the status
 *		of the timer (the timer "object")
 *	  time
 *		the elapsed time of the timer is put in 'time'
 '		in milliseconds
 *
 * Return value:
 *     0  - success
 *    !0  - error
 */
int readTimer(struct timertype_t *timer, __int64 *time) {

    __int64 timeDiff;

    if (timer->running == 1) {

        __int64 timeCurrent;
        readRunningTimer(timer, &timeCurrent);
        timeDiff = timeCurrent - timer->time;
    }
    else {

        /* timer not running */
        /* the elapsed time was already set in 'stopTimer' */
        timeDiff = timer->time;
    }

    *time = timeDiff * timer->userFrequency / timer->systemFrequency;
    return 0;
}

/*##**********************************************\
 *
 *      getSystemTicker
 *
 * The current system ticker value (WIN32) put in
 * the reference parameter tickerValue. For the platforms
 * using 'gettimeofday' the '.tv_sec * .tv_usec' value is
 * set to tickerValue (microseconds since the Epoch)
 *
 * Parameters :
 *    timer
 *		current system ticker / timeofday value set by
 *		this method
 *
 * Return value:
 *     0  - success
 *    !0  - error
 */
 int getSystemTicker(__int64 *tickerValue)
 {
#ifdef WIN32
    LARGE_INTEGER ticker;
	/* Get the frequency of the high-resolution
		performance counter */
	if (!QueryPerformanceCounter(&ticker)) {
       return -1;
	}
	*tickerValue = ticker.QuadPart;
    return 0;
#else
	struct timeval timev;
	if (gettimeofday(&timev, NULL) == 0) {
		/* Use microseconds since the Epoch */
		*tickerValue =  (__int64) timev.tv_sec * MILLION + timev.tv_usec;
		return 0;
    }
	else {
		/* could not get time of day for some reason */
		return -1;
    }
#endif
 }

/*##**********************************************\
 *
 *      estimateTimerResolution
 *
 * Estimates the minimal difference between two
 * adjucent timer values.
 *
 * Parameters :
 *    timer
 *      initialized, stopped timer object, timer state
 *      isn't changed during examination.
 *
 *    timerResMicrosec
 *      estimated timer resolution, in seconds
 *
 * Return value:
 *     0  - success
 *    !0  - error
 */
int estimateTimerResolution(
    struct timertype_t *timer,
    double *timerResolutionMean) {

    __int64 start, end, v, old, count;

    readRunningTimer(timer, &start);

    end = start + timer->systemFrequency; /* one second */
    count = 1;
    old = start;

    do {
        readRunningTimer(timer, &v);
        if (v != old) {

            old = v;
            ++count;
        }
    } while (v <= end);

    *timerResolutionMean =
        1.0 / (end - start) / count * timer->systemFrequency;

    return 0;
}
