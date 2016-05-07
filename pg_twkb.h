/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 *
 * Copyright (C) 2013 Nicklas Avén
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the LICENCE file.
 *
 **********************************************************************/
 
 #include "postgres.h"

#include <math.h>
#include <float.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <assert.h>

#include "access/gist.h"
#include "access/itup.h"

#include "fmgr.h"
#include "utils/elog.h"
#include "mb/pg_wchar.h"
# include "lib/stringinfo.h" /* for binary input */
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "funcapi.h"

#include "liblwgeom.h"

#include "varint.h"
#include "bytebuffer.h"

#if POSTGIS_PGSQL_VERSION > 92
#include "access/htup_details.h"
#endif

#define POSTGIS_PGSQL_VERSION 95

#define WKB_POINT_TYPE 1
#define WKB_LINESTRING_TYPE 2
#define WKB_POLYGON_TYPE 3
#define WKB_MULTIPOINT_TYPE 4
#define WKB_MULTILINESTRING_TYPE 5
#define WKB_MULTIPOLYGON_TYPE 6
#define WKB_GEOMETRYCOLLECTION_TYPE 7

/**
 * Write a notice out to the notice handler.
 *
 * Uses standard printf() substitutions.
 * Use for messages you always want output.
 * For debugging, use LWDEBUG() or LWDEBUGF().
 * @ingroup logging
 */
void lwnotice(const char *fmt, ...);

/**
 * Write a notice out to the error handler.
 *
 * Uses standard printf() substitutions.
 * Use for errors you always want output.
 * For debugging, use LWDEBUG() or LWDEBUGF().
 * @ingroup logging
 */
void lwerror(const char *fmt, ...);

//extern uint8_t* twkb_to_twkbcoll(uint8_t **twkb, size_t *sizes,size_t *out_size, int64_t *idlist, int n);

uint8_t* twkb_to_twkbcoll(uint8_t **twkb, size_t *sizes,size_t *out_size, int64_t *idlist, int n);

int getsqlitetype(char *pgtype, char *sqlitetype);
int write2sqlite(char* sql_string, char* sqlitedb_name);
