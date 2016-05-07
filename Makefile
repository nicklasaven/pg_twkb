

LDFLAGS =  -L /usr/local/lib -l lwgeom -lsqlite3

MODULE_big = pg_twkb
OBJS = pg_twkb.o\
		twkb_tools.o \
		lwin_twkb.o \
		lwout_twkb.o \
		sqlite_writer.o \

EXTENSION = pg_twkb
DATA = pg_twkb--0.1.sql

EXTRA-CLEAN =

#PG_CONFIG = pg_config
PG_CONFIG =/usr/lib/postgresql/9.5/bin/pg_config

CFLAGS += $(shell $(CURL_CONFIG) --cflags)

LIBS += $(LDFLAGS)
SHLIB_LINK := $(LIBS)

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
