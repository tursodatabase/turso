# set the following paths and files according to your environment

UNIXODBC_INCLUDEPATH = /usr/local/include/unixODBC-2.2.14/include
SOLID_INCLUDEPATH = /opt/solidDB/soliddb-7.0/include
SOLID_DYNAMIC_CLIENTLIB = sacl2x6470.so
SOLID_STATIC_CLIENTLIB = solidodbca.a
SOLID_DYNAMIC_ACCELERATORLIB = ssolidac70.so
SOLID_STATIC_ACCELERATORLIB = solidac.a
SOLID_DYNAMIC_SMALIB = ssolidsma70.so

# how to build different versions:
#
# make DRIVERTYPE=unixodbc        with UnixODBC
# make DRIVERTYPE=soliddynamic    dynamically linked to the solidDB driver
# make DRIVERTYPE=solidstatic     directly linked to the solidDB driver
#
# make SERVERTYPE=accelerator     dynamically linked to the solidDB accelerator 
#                                 library (LLA)
#
# make SERVERTYPE=accelerator DRIVERTYPE=solidstatic   
#                                 directly linked to the solidDB accelerator library (LLA)
# make SERVERTYPE=sma             dynamically linked to the solidDB SMA library
#
# make                            uses default settings: 
#                                 dynamically linked normal ODBC client driver
#

CC = cc 
CFLAGS = -Wall -pthread 

#ifndef SERVERTYPE
SERVERTYPE = clientserver
#SERVERTYPE = accelerator
#SERVERTYPE = sma
#endif

#ifndef DRIVERTYPE
DRIVERTYPE = soliddynamic
#endif

ifeq ($(SERVERTYPE),clientserver)
SOURCES_SERVER = server_dummy.c
ifeq ($(DRIVERTYPE),unixodbc)
# unixODBC version
ALL_CFLAGS = -DBUILD_REAL_64_BIT_MODE $(CFLAGS)
INCLUDES = -I$(UNIXODBC_INCLUDEPATH)
LDFLAGS = -lodbc -lm
endif
ifeq ($(DRIVERTYPE),solidstatic)
ALL_CFLAGS = -DSOLID_BUILD -DUNIX_64BIT $(CFLAGS)
INCLUDES = -I$(SOLID_INCLUDEPATH)
LDFLAGS = $(SOLID_STATIC_CLIENTLIB) -lm 
endif
ifeq ($(DRIVERTYPE),soliddynamic)
ALL_CFLAGS = -DSOLID_BUILD -DUNIX_64BIT $(CFLAGS)
INCLUDES = -I$(SOLID_INCLUDEPATH)
LDFLAGS = $(SOLID_DYNAMIC_CLIENTLIB) -lc -lm -lrt -lnsl
endif
endif

ifeq ($(SERVERTYPE), accelerator)
SOURCES_SERVER = solid_accelerator.c
ifeq ($(DRIVERTYPE),soliddynamic)
# add -DCACHE_MODE to wait for user input before starting/stopping test
ALL_CFLAGS = -DACCELERATOR -DSOLID_BUILD -DUNIX_64BIT $(CFLAGS)
INCLUDES = -I$(SOLID_INCLUDEPATH)
LDFLAGS = $(SOLID_DYNAMIC_ACCELERATORLIB) -lm -ldl
endif
ifeq ($(DRIVERTYPE),solidstatic)
# add -DCACHE_MODE to wait for user input before starting/stopping test
ALL_CFLAGS = -DACCELERATOR -DSOLID_BUILD -DUNIX_64BIT $(CFLAGS)
INCLUDES = -I$(SOLID_INCLUDEPATH)
LDFLAGS = $(SOLID_STATIC_ACCELERATORLIB) -lm -ldl
endif
endif

ifeq ($(SERVERTYPE), sma)
ALL_CFLAGS = -DSOLID_BUILD -DUNIX_64BIT $(CFLAGS)
SOURCES_SERVER = server_dummy.c
INCLUDES = -I$(SOLID_INCLUDEPATH)
LDFLAGS = $(SOLID_DYNAMIC_SMALIB) -lm -ldl
endif
include Makefile.inc
