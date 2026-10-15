# source files for all modules
SOURCES = client.c communication.c control.c remcontrol.c statistics.c timer.c util.c mt19937ar.c columnvalues.c
SOURCES_CONTROL = control.c tirdb.c remcontrol.c util.c thd.c communication.c timer.c mt19937ar.c linuxspec.c columnvalues.c random.c $(SOURCES_SERVER)
SOURCES_CLIENT = client.c util.c communication.c timer.c mt19937ar.c linuxspec.c columnvalues.c random.c thd.c targetdb.c $(SOURCES_SERVER)
SOURCES_STATISTICS = statistics.c thd.c communication.c util.c timer.c $(SOURCES_SERVER)

DEBUGFLAGS = -g -ggdb -D_DEBUG

# rules for creating binaries
all : client tatp statistics

alldebug : clientdebug tatpdebug statisticsdebug

client : $(SOURCES_CLIENT)
	$(CC) $(INCLUDES) $(ALL_CFLAGS) -o client $(SOURCES_CLIENT) $(LDFLAGS)

tatp : $(SOURCES_CONTROL)
	$(CC) $(INCLUDES) $(ALL_CFLAGS) -o tatp $(SOURCES_CONTROL) $(LDFLAGS)

statistics : $(SOURCES_STATISTICS)
	$(CC) $(INCLUDES) $(ALL_CFLAGS) -o statistics $(SOURCES_STATISTICS) $(LDFLAGS)

clientdebug : $(SOURCES_CLIENT)
	$(CC) $(DEBUGFLAGS) $(INCLUDES) $(ALL_CFLAGS) -o client $(SOURCES_CLIENT) $(LDFLAGS)

tatpdebug : $(SOURCES_CONTROL)
	$(CC) $(DEBUGFLAGS) $(INCLUDES) $(ALL_CFLAGS) -o tatp $(SOURCES_CONTROL) $(LDFLAGS)

statisticsdebug : $(SOURCES_STATISTICS)
	$(CC) $(DEBUGFLAGS) $(INCLUDES) $(ALL_CFLAGS) -o statistics $(SOURCES_STATISTICS) $(LDFLAGS)

# run 'make clean' to remove all object files and executables
clean:
	rm -f *.o core *.tmp client tatp statistics

debug : clean

debug :	alldebug
