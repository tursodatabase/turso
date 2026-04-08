#ifndef _SQLITE_SRC_TIMERS_H_
#define _SQLITE_SRC_TIMERS_H_

#include <sys/time.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>

enum instrumentation_vars {
	open_t,      
	close_t,
	pread_t,
	pwrite_t,
	read_t,
	write_t,
	seek_t,
	fsync_t,
	unlink_t,
	bg_thread_t,
	memcpy_to_pmem_t,
	fsync_noop_t,
	neword_t,
	payment_t,
	ordstat_t,
	delivery_t,
	slev_t,
	INSTRUMENT_NUM,
};

atomic_uint_least64_t Instrustats[INSTRUMENT_NUM];
static const char *Instruprint[INSTRUMENT_NUM] =
{
	"open",
	"close",
	"pread",
	"pwrite",
	"read",
	"write",
	"seek",
	"fsync",
	"unlink",
	"bg_thread",
	"memcpy_to_pmem",
	"fsync_noop",
	"neword",
	"payment",
	"ordstat",
	"delivery",
	"slev",
};

typedef struct timespec instrumentation_type;

#define INSTRUMENT_CALLS 1


#define INITIALIZE_TIMERS()					\
	{							\
		int i;						\
		for (i = 0; i < INSTRUMENT_NUM; i++)		\
			Instrustats[i] = 0;			\
	}							\

#if INSTRUMENT_CALLS

#define START_TIMING(name, start)				\
	{							\
		clock_gettime(CLOCK_MONOTONIC, &start);		\
        }

#define END_TIMING(name, start)						\
	{                                                               \
		instrumentation_type end;				\
		clock_gettime(CLOCK_MONOTONIC, &end);			\
		__atomic_fetch_add(&Instrustats[name], (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec), __ATOMIC_SEQ_CST); \
        }

#define PRINT_TIME()							\
	{								\
		int i;							\
		printf("\n ----------------------\n");			\
		for(i=0; i<INSTRUMENT_NUM; i++)				\
			if (Instrustats[i] > 0)				\
				printf("%s: timing = %lu nanoseconds\n", \
				       Instruprint[i], Instrustats[i]);	\
	}


#else

#define START_TIMING(name, start) {(void)(start);}
#define END_TIMING(name, start) {(void)(start);}
#define PRINT_TIME() {(void)(Instrustats[0]);}
	
#endif

#endif
