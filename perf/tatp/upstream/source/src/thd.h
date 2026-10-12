/**********************************************************************\
**  source      * thd.h
**  description * Thread related functions
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public 
**  License 1.0 as published by the Open Source Initiative (OSI). 
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#ifndef _THD_H
#define _THD_H

#ifdef WIN32

#include <windows.h>

/*
 *********************************************************************
 *    Portable Mutex abstraction
 *********************************************************************
 */
typedef struct
{
    CRITICAL_SECTION cs;
} thd_mutex_t;

/*
 *********************************************************************
 *    Portable Event abstraction
 *********************************************************************
 */
typedef struct
{
    HANDLE  event_handle;
} thd_event_t;

/* Portable argument to thread function */
typedef void*   THD_ARG;

/* Portable thread return type */
typedef ULONG THD_RET;

/* Portable thread function modifier */
#define THD_FUN WINAPI

/*
 *********************************************************************
 *    Portable thread abstraction
 *********************************************************************
 */
typedef struct
{
    HANDLE  thread_handle;
} thd_thread_t;

/*
 *********************************************************************
 *    Portable thread local variable abstraction
 *********************************************************************
 */
typedef struct
{
    DWORD index;
} thd_tls_t;

#elif defined __GNUC__ && defined __linux || \
	(defined __SUNPRO_C || defined __SUNPRO_CC) && defined __sun || \
	defined __xlC__ && defined __TOS_AIX__ || \
	defined __hpux

#include <pthread.h>

/*
 *********************************************************************
 *    Portable Mutex abstraction
 *********************************************************************
 */
typedef struct
{
    pthread_mutex_t mutex;
} thd_mutex_t;

/*
 *********************************************************************
 *    Portable Event abstraction
 *********************************************************************
 */
typedef struct
{
    int signal_all;
    int predicate;
    pthread_mutex_t mutex;
    pthread_cond_t  condvar;
} thd_event_t;

/* Portable argument to thread function */
typedef void*   THD_ARG;

/* Portable thread return type */
typedef void*   THD_RET;

/* Portable thread function modifier */
#define THD_FUN

/*
 *********************************************************************
 *    Portable thread abstraction
 *********************************************************************
 */
typedef struct
{
    pthread_t thread_id;
} thd_thread_t;

/*
 *********************************************************************
 *    Portable thread local variable abstraction
 *********************************************************************
 */
typedef struct
{
    pthread_key_t key;
} thd_tls_t;

#endif

int thd_mutex_create(thd_mutex_t* );
int thd_mutex_destroy(thd_mutex_t* );
int thd_mutex_lock(thd_mutex_t* );
int thd_mutex_unlock(thd_mutex_t* );

int thd_event_create(thd_event_t* , int signal_all, int initial_state);
int thd_event_destroy(thd_event_t* );
int thd_event_wait(thd_event_t* );
int thd_event_signal(thd_event_t* );
int thd_event_reset(thd_event_t* );

typedef THD_RET (THD_FUN *thd_function_t) (THD_ARG);

int thd_thread_init(thd_thread_t* );
int thd_thread_destroy(thd_thread_t* );
int thd_thread_start(thd_thread_t* , thd_function_t function, THD_ARG arg);
int thd_thread_join(thd_thread_t* );
int thd_self_id();

typedef const void* THD_TLS_VALUE_T;

int thd_tls_create(thd_tls_t* );
int thd_tls_destroy(thd_tls_t* );
int thd_tls_set(thd_tls_t* , THD_TLS_VALUE_T );
THD_TLS_VALUE_T thd_tls_get(thd_tls_t* );

int thd_sleep(int milliseconds);

#endif  /* _THD_H */
