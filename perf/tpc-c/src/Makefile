#
# "make all" to build necessary executables.
#
# Build with turso (default):  make all
# Build with system sqlite3:   make all BACKEND=sqlite
#

BACKEND ?= turso

TURSO_ROOT ?=	../../..
TURSO_INC=	$(TURSO_ROOT)/sqlite3/include
TURSO_PROFILE ?= release
TURSO_LIB=	$(TURSO_ROOT)/target/$(TURSO_PROFILE)

ifeq ($(BACKEND),turso)
  LIBS=		-lrt -L$(TURSO_LIB) -lturso_sqlite3 -lpthread -lm -ldl
  RPATH=	-Wl,-rpath,$(shell realpath $(TURSO_LIB))
  INC=		-I. -I$(TURSO_INC)
  SUFFIX=
  DB_NAME=	tpcc.db
else
  LIBS=		-lrt -lsqlite3 -lpthread -lm
  RPATH=
  INC=		-I.
  SUFFIX=	-sqlite
  DB_NAME=	tpcc-sqlite.db
endif

#DEFS=		-DDEBUG

CFLAGS=		-w -O3 -g -std=gnu11 -fcommon -DDB_PATH='"$(DB_NAME)"'

TRANSACTIONS=	neword.o payment.o ordstat.o delivery.o slev.o
OBJS=		main.o spt_proc.o driver.o support.o sequence.o rthist.o sb_percentile.o $(TRANSACTIONS)

.SUFFIXES:
.SUFFIXES: .o .c

.c.o:
	$(CC) $(CFLAGS) $(INC) $(DEFS) -c $*.c

all: ../tpcc_load$(SUFFIX) ../tpcc_start$(SUFFIX)

../tpcc_load$(SUFFIX) : load.o support.o
	$(CC) load.o support.o $(LIBS) $(RPATH) -o $@

../tpcc_start$(SUFFIX) : $(OBJS)
	$(CC) $(OBJS) $(LIBS) $(RPATH) -o $@

turso:
	cd $(TURSO_ROOT) && cargo build -p turso_sqlite3 --quiet $(if $(filter release,$(TURSO_PROFILE)),--release,)

load: ../tpcc_load$(SUFFIX)
	cd .. && rm -f $(DB_NAME) $(DB_NAME)-wal $(DB_NAME)-shm $(DB_NAME)-journal \
		&& sqlite3 $(DB_NAME) < create_table_sqlite.sql \
		&& sqlite3 $(DB_NAME) < add_fkey_idx_sqlite.sql \
		&& ./tpcc_load$(SUFFIX) -w $(or $(WAREHOUSES),1)

bench: ../tpcc_start$(SUFFIX)
	cd .. && ./tpcc_start$(SUFFIX) \
		-w $(or $(WAREHOUSES),1) \
		-c $(or $(CONNECTIONS),1) \
		-r $(or $(WARMUP),5) \
		-l $(or $(MEASURE),30) \
		-i $(or $(INTERVAL),1)

clean :
	rm -f *.o
