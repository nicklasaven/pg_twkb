/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 *
 *
 * Copyright (C) 2015 Paul Ramsey
 * Copyright (C) 2014 Nicklas Av�n
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the LICENCE file.
 **********************************************************************/



#include <limits.h>
#include "pg_twkb.h"

/* Maximum number of geometry dimmensions that internal arrays can hold */
#define MAX_N_DIMS 4

#define MAX_BBOX_SIZE 64
#define MAX_SIZE_SIZE 8


/**
* Header true/false flags
*/

#define FIRST_BYTE_SET_BBOXES(flag, bool)   ((flag) = ((bool) ? (flag) | 0x01 : (flag) & (~0x01)))
#define FIRST_BYTE_SET_SIZES(flag, bool)    ((flag) = ((bool) ? (flag) | 0x02 : (flag) & (~0x02)))
#define FIRST_BYTE_SET_IDLIST(flag, bool)   ((flag) = ((bool) ? (flag) | 0x04 : (flag) & (~0x04)))
#define FIRST_BYTE_SET_EXTENDED(flag, bool) ((flag) = ((bool) ? (flag) | 0x08 : (flag) & (~0x08)))
#define FIRST_BYTE_SET_EMPTY(flag, bool)    ((flag) = ((bool) ? (flag) | 0x10 : (flag) & (~0x10)))


/**
* Macros for manipulating the 'type_precision' int. An int8_t used as follows:
* Type 4 bits
* Precision 4 bits
*/

#define TYPE_PREC_SET_TYPE(flag, type) ((flag) = ((flag) & 0xF0) | (((type) & 0x0F)))
#define TYPE_PREC_SET_PREC(flag, prec) ((flag) = ((flag) & 0x0F) | (((prec) & 0x0F) << 4))

#define HIGHER_DIM_SET_HASZ(flag, bool) ((flag) = ((bool) ? (flag) | 0x01 : (flag) & (~0x01)))
#define HIGHER_DIM_SET_HASM(flag, bool) ((flag) = ((bool) ? (flag) | 0x02 : (flag) & (~0x02)))

#define HIGHER_DIM_SET_PRECZ(flag, prec) ((flag) = ((flag) & 0xE3) | (((prec) & 0x07) << 2))
#define HIGHER_DIM_SET_PRECM(flag, prec) ((flag) = ((flag) & 0x1F) | (((prec) & 0x07) << 5))

typedef struct
{
	/* Options defined at start */
	uint8_t variant;
	int8_t prec_xy;
	int8_t prec_z;
	int8_t prec_m;
	float factor[4]; /*What factor to multiply the coordiinates with to get the requested precision*/
} TWKB_GLOBALS;

typedef struct
{
	uint8_t variant;  /*options that change at runtime*/
	bytebuffer_t *header_buf;
	bytebuffer_t *geom_buf;
	int hasz;
	int hasm;
	const int64_t *idlist;
	int64_t bbox_min[MAX_N_DIMS];
	int64_t bbox_max[MAX_N_DIMS];
	int64_t accum_rels[MAX_N_DIMS]; /*Holds the acculmulated relative values*/
} TWKB_STATE;

int lwgeom_to_twkb_buf(const LWGEOM *geom, TWKB_GLOBALS *global_values, TWKB_STATE *ts);

int lwpoint_to_twkb_buf(const LWPOINT *line, TWKB_GLOBALS *global_values, TWKB_STATE *ts);
int lwline_to_twkb_buf(const LWLINE *line, TWKB_GLOBALS *global_values, TWKB_STATE *ts);
int lwpoly_to_twkb_buf(const LWPOLY *poly, TWKB_GLOBALS *global_values, TWKB_STATE *ts);
int lwmulti_to_twkb_buf(const LWCOLLECTION *col, TWKB_GLOBALS *globals, TWKB_STATE *ts);
int lwcollection_to_twkb_buf(const LWCOLLECTION *col, TWKB_GLOBALS *global_values, TWKB_STATE *ts);
int lwgeom_write_to_buffer(const LWGEOM *geom, TWKB_GLOBALS *global_values, TWKB_STATE *parent_state);

size_t sizeof_bbox(TWKB_STATE *ts, int ndims);
void write_bbox(TWKB_STATE *ts, int ndims);