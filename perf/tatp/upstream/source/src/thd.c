/**********************************************************************\
**  source       * thd.c
**  description  * Thread related functions
**
**
**  Copyright IBM Corporation 2004, 2011.
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#include "thd.h"

#ifdef WIN32
#define THD_USE_WIN32_THREADS
#ifndef THD_USE_CREATE_THREAD_API
#include <process.h>    /* _beginthreadex */
#endif
#endif

#if defined __GNUC__ && defined __linux || \
	(defined __SUNPRO_C || defined __SUNPRO_CC) && defined __sun || \
	defined __xlC__ && defined __TOS_AIX__ || \
	defined __hpux
#define THD_USE_PTHREADS
#include <errno.h>
#endif

#ifdef THD_USE_WIN32_THREADS

/*##**********************************************************************\
 *
 *      thd_mutex_create
 *
 * Parameters:
 *      mutex 
 *
 * Return value:
 *      0 - always
 */
int thd_mutex_create(thd_mutex_t* mutex)
{
        InitializeCriticalSection(&mutex->cs);
        return 0;
}

/*##**********************************************************************\
 *
 *      thd_mutex_destroy
 *
 * Parameters:
 *      mutex 
 *
 * Return value:
 *      0 - always
 */
int thd_mutex_destroy(thd_mutex_t* mutex)
{
        DeleteCriticalSection(&mutex->cs);
        return 0;
}

/*##**********************************************************************\
 *
 *      thd_mutex_lock
 *
 * Parameters:
 *      mutex 
 *
 * Return value:
 *      0 - always
 */
int thd_mutex_lock(thd_mutex_t* mutex)
{
        EnterCriticalSection(&mutex->cs);
        return 0;
}

/*##**********************************************************************\
 *
 *      thd_mutex_unlock
 *
 * Parameters:
 *      mutex 
 *
 * Return value:
 *      0 - always
 */
int thd_mutex_unlock(thd_mutex_t* mutex)
{
        LeaveCriticalSection(&mutex->cs);
        return 0;
}
#endif

#ifdef THD_USE_PTHREADS

/*##**********************************************************************\
 *
 *      thd_mutex_create
 *
 * Parameters:
 *      mutex 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_mutex_create(thd_mutex_t* mutex)
{
        return pthread_mutex_init(&mutex->mutex, NULL);
}

/*##**********************************************************************\
 *
 *      thd_mutex_destroy
 *
 * Parameters:
 *      mutex 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_mutex_destroy(thd_mutex_t* mutex)
{
        return pthread_mutex_destroy(&mutex->mutex);
}

/*##**********************************************************************\
 *
 *      thd_mutex_lock
 *
 * Parameters:
 *      mutex
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_mutex_lock(thd_mutex_t* mutex)
{
        return pthread_mutex_lock(&mutex->mutex);
}

/*##**********************************************************************\
 *
 *      thd_mutex_unlock
 *
 * Parameters:
 *      mutex 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_mutex_unlock(thd_mutex_t* mutex)
{
        return pthread_mutex_unlock(&mutex->mutex);
}
#endif

#ifdef THD_USE_WIN32_THREADS

/*##**********************************************************************\
 *
 *      thd_event_create
 *
 * Parameters:
 *      event 
 *
 *      signal_all 
 *
 *      initial_state 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_event_create(thd_event_t* event, int signal_all, int initial_state)
{
        HANDLE  h = CreateEvent(NULL, signal_all ? TRUE : FALSE,
                                initial_state ? TRUE : FALSE,
                                NULL);
        
        if (h != NULL) {
            event->event_handle = h;
            return 0;
        }
        
        return GetLastError();
}

/*##**********************************************************************\
 *
 *      thd_event_destroy
 *
 * Parameters:
 *      event 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_event_destroy(thd_event_t* event)
{
        BOOL ok;
        ok = CloseHandle(event->event_handle);
        return ok ? 0 : GetLastError();
}

/*##**********************************************************************\
 *
 *      thd_event_wait
 *
 * Parameters:
 *      event 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_event_wait(thd_event_t* event)
{
        ULONG ret;
        ret = WaitForSingleObject(event->event_handle, INFINITE);
        return (ret == WAIT_OBJECT_0) ? 0 : GetLastError();
}

/*##**********************************************************************\
 *
 *      thd_event_signal
 *
 * Parameters:
 *      event 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_event_signal(thd_event_t* event)
{
        BOOL ok;
        ok = SetEvent(event->event_handle);
        return ok ? 0 : GetLastError();
}

/*##**********************************************************************\
 *
 *      thd_event_reset
 *
 * Parameters:
 *      event 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_event_reset(thd_event_t* event)
{
        BOOL ok;
        ok = ResetEvent(event->event_handle);
        return ok ? 0 : GetLastError();
}
#endif

#ifdef THD_USE_PTHREADS

/*##**********************************************************************\
 *
 *      thd_event_create
 *
 * Parameters:
 *      event 
 *
 *      signal_all 
 *
 *      initial_state 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_event_create(thd_event_t* event, int signal_all, int initial_state)
{
        int ret;
        pthread_mutex_t mutex;
        pthread_cond_t  condvar;
        
        ret = pthread_cond_init(&condvar, NULL);
        if (ret == 0) {
            ret = pthread_mutex_init(&mutex, NULL);
            if (ret == 0) {
                event->signal_all = signal_all;
                event->predicate = initial_state;
                event->mutex = mutex;
                event->condvar = condvar;
                return 0;
            }
            
            pthread_cond_destroy(&condvar);
        }
        
        return ret;
}

/*##**********************************************************************\
 *
 *      thd_event_destroy
 *
 * Parameters:
 *      event 
 *
 * Return value:
 *      0 - always
 */
int thd_event_destroy(thd_event_t* event)
{
        int ret;
        ret = pthread_cond_destroy(&event->condvar);
        ret = pthread_mutex_destroy(&event->mutex);
        return 0;   /* actually nothing we can do */
}

/*##**********************************************************************\
 *
 *      thd_event_wait
 *
 * Parameters:
 *      event 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_event_wait(thd_event_t* event)
{
        int ret;
        ret = pthread_mutex_lock(&event->mutex);
        if (ret == 0) {
            while (ret == 0 && !event->predicate) {
                ret = pthread_cond_wait(&event->condvar, &event->mutex);
            }            
            pthread_mutex_unlock(&event->mutex);
        }        
    return ret;
}

/*##**********************************************************************\
 *
 *      thd_event_signal
 *
 * Parameters:
 *      event 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_event_signal(thd_event_t* event)
{
        int ret;
        ret = pthread_mutex_lock(&event->mutex);
        if (ret == 0) {
            event->predicate = 1;
            ret = event->signal_all ? pthread_cond_broadcast(&event->condvar)
                : pthread_cond_signal(&event->condvar);
            
            /* ret = */
            pthread_mutex_unlock(&event->mutex);
        }        
    return ret;
}

/*##**********************************************************************\
 *
 *      thd_event_reset
 *
 * Parameters:
 *      event 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_event_reset(thd_event_t* event)
{
        int ret;
        ret = pthread_mutex_lock(&event->mutex);
        if (ret == 0) {
            event->predicate = 0;
            
            /* ret = */
            pthread_mutex_unlock(&event->mutex);
        }
        
        return ret;
}
#endif

#ifdef THD_USE_WIN32_THREADS

/*##**********************************************************************\
 *
 *      thd_thread_init
 *
 * Parameters:
 *      thread 
 *
 * Return value:
 *      0 - always
 */
int thd_thread_init(thd_thread_t* thread)
{
        thread->thread_handle = NULL;
        return 0;
}

/*##**********************************************************************\
 *
 *      thd_thread_destroy
 *
 * Parameters:
 *      thread 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_thread_destroy(thd_thread_t* thread)
{
    BOOL ok = TRUE;
    ok = CloseHandle(thread->thread_handle);
    if (ok) {
        thread->thread_handle = NULL;
        return 0;
    }

    return GetLastError();
}

/*##**********************************************************************\
 *
 *      thd_thread_start
 *
 * Parameters:
 *      thread 
 *
 *      function 
 *
 *      arg 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_thread_start(thd_thread_t* thread, thd_function_t function,
                     THD_ARG arg)
{
    HANDLE  thread_handle;

#ifdef THD_USE_CREATE_THREAD_API
    thread_handle = CreateThread(NULL, 0, function, arg, 0, NULL);
#else
    thread_handle = (HANDLE) _beginthreadex(NULL, 0,
        (unsigned (WINAPI *)(THD_ARG)) function,
        arg, 0, NULL);
#endif
    if (thread_handle != NULL) {
        thread->thread_handle = thread_handle;
        return 0;
    }

    return GetLastError();
}

/*##**********************************************************************\
 *
 *      thd_thread_join
 *
 * Parameters:
 *      thread 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_thread_join(thd_thread_t* thread)
{
        if (thread->thread_handle != NULL) {
            ULONG ret = WaitForSingleObject(thread->thread_handle, INFINITE);
            if (ret != WAIT_OBJECT_0) {
                return GetLastError();
            }
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      thd_sleep
 *
 * Parameters:
 *      milliseconds 
 *
 * Return value:
 *      thread id
 */
int thd_self_id()
{
        return GetCurrentThreadId();
}

/*##**********************************************************************\
 *
 *      thd_sleep
 *
 * Parameters:
 *      milliseconds 
 *
 * Return value:
 *      0 - always
 */
int thd_sleep(int milliseconds)
{
        Sleep(milliseconds);
        return 0;
}
#endif

#ifdef THD_USE_PTHREADS

/*##**********************************************************************\
 *
 *      thd_thread_init
 *
 * Parameters:
 *      thread 
 *
 * Return value:
 *      0 - always
 */
int thd_thread_init(thd_thread_t* thread)
{
        return 0;
}

/*##**********************************************************************\
 *
 *      thd_thread_destroy
 *
 * Parameters:
 *      thread 
 *
 * Return value:
 *      0 - always
 */
int thd_thread_destroy(thd_thread_t* thread)
{
        return 0;
}

/*##**********************************************************************\
 *
 *      thd_thread_start
 *
 * Parameters:
 *      thread 
 *
 *      function 
 *
 *      arg 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_thread_start(thd_thread_t* thread, thd_function_t function,
                     THD_ARG arg)
{
    int ret;

    pthread_t thread_id;
    ret = pthread_create(&thread_id, NULL, function, arg);

    if (ret == 0) {
        thread->thread_id = thread_id;
    }

    return ret;
}

/*##**********************************************************************\
 *
 *      thd_thread_join
 *
 * Parameters:
 *      thread 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_thread_join(thd_thread_t* thread)
{
    return pthread_join(thread->thread_id, NULL);
}

/*##**********************************************************************\
 *
 *      return
 *
 * Parameters:
 *      int 
 *
 * Return value:
 *      id of the calling thread
 */
int thd_self_id()
{        
    return (int)pthread_self();
}

/*##**********************************************************************\
 *
 *      thd_sleep
 *
 * Parameters:
 *      milliseconds 
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int thd_sleep(int milliseconds)
{
        struct timespec rqtp;

        rqtp.tv_sec = milliseconds / 1000;
        rqtp.tv_nsec = (milliseconds % 1000) * 1000000;
    
        for (;;) {
            int ret;
            struct timespec rmtp;
            
            ret = nanosleep(&rqtp, &rmtp);
            
            if (ret == 0) {
                break;
            } else if (ret == -1 && errno == EINTR) {
                rqtp = rmtp;
            } else {
                return errno;
            }
        }
        
        return 0;
}
#endif

#ifdef THD_USE_WIN32_THREADS

/*##**********************************************************************\
 *
 *      thd_tls_create
 *
 * Parameters:
 *      tls 
 *
 * Return value:
 *      0 - success
 *     !0 - error 
 */
int thd_tls_create(thd_tls_t* tls)
{
        
        DWORD index;
        index = TlsAlloc();
        if (index != TLS_OUT_OF_INDEXES) {
            tls->index = index;
            return 0;
        }
        
        return GetLastError();
}

/*##**********************************************************************\
 *
 *      thd_tls_destroy
 *
 * Parameters:
 *      tls 
 *
 * Return value:
 *      0 - success
 *     !0 - error 
 */
int thd_tls_destroy(thd_tls_t* tls)
{
        return TlsFree(tls->index) ? 0 : GetLastError();
}

/*##**********************************************************************\
 *
 *      thd_tls_set
 *
 * Parameters:
 *      tls 
 *
 *      val 
 *
 * Return value:
 *      0 - success
 *     !0 - error 
 */
int thd_tls_set(thd_tls_t* tls, THD_TLS_VALUE_T val)
{
        return TlsSetValue(tls->index, (PVOID) val) ? 0 : GetLastError();
}

/*##**********************************************************************\
 *
 *      thd_tls_get
 *
 * Parameters:
 *      tls 
 *
 * Return value:
 *      0 - success
 *     !0 - error 
 */
THD_TLS_VALUE_T thd_tls_get(thd_tls_t* tls)
{
        return (THD_TLS_VALUE_T) TlsGetValue(tls->index);
}
#endif

#ifdef THD_USE_PTHREADS

/*##**********************************************************************\
 *
 *      thd_tls_create
 *
 * Parameters:
 *      tls 
 *
 * Return value:
 *      0 - success
 *     !0 - error 
 */
int thd_tls_create(thd_tls_t* tls)
{
        return pthread_key_create(&tls->key, NULL);
}

/*##**********************************************************************\
 *
 *      thd_tls_destroy
 *
 * Parameters:
 *      tls 
 *
 * Return value:
 *      0 - success
 *     !0 - error 
 */
int thd_tls_destroy(thd_tls_t* tls)
{
        return pthread_key_delete(tls->key);
}

/*##**********************************************************************\
 *
 *      thd_tls_set
 *
 * Parameters:
 *      tls 
 *
 *      val 
 *
 * Return value:
 *      0 - success
 *     !0 - error 
 */
int thd_tls_set(thd_tls_t* tls, THD_TLS_VALUE_T val)
{
        return pthread_setspecific(tls->key, (const void *) val);
}

/*##**********************************************************************\
 *
 *      thd_tls_get
 *
 * Parameters:
 *      tls
 *
 * Return value:
 *      thread-specific data value associated with the given key
 *      NULL, if no value was associated
 */
THD_TLS_VALUE_T thd_tls_get(thd_tls_t* tls)
{
        return (THD_TLS_VALUE_T) pthread_getspecific(tls->key);
}
#endif
