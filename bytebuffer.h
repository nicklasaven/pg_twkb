/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 *
 * Copyright (C) 2015 Nicklas Avén
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the LICENCE file.
 *
 **********************************************************************/

#ifndef _BYTEBUFFER_H
#define _BYTEBUFFER_H 1

#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include "liblwgeom.h"

#define BYTEBUFFER_STARTSIZE 128


typedef struct
{
	size_t capacity;
	uint8_t *buf_start;
	uint8_t *writecursor;	
	uint8_t *readcursor;	
}
bytebuffer_t;

void bytebuffer_init_with_size(bytebuffer_t *b, size_t size);
bytebuffer_t *bytebuffer_create_with_size(size_t size);
bytebuffer_t *bytebuffer_create(void);
void bytebuffer_destroy(bytebuffer_t *s);
void bytebuffer_clear(bytebuffer_t *s);
void bytebuffer_append_byte(bytebuffer_t *s, const uint8_t val);
void bytebuffer_append_varint(bytebuffer_t *s, const int64_t val);
void bytebuffer_append_uvarint(bytebuffer_t *s, const uint64_t val);
uint64_t bytebuffer_read_uvarint(bytebuffer_t *s);
int64_t bytebuffer_read_varint(bytebuffer_t *s);
size_t bytebuffer_getlength(bytebuffer_t *s);
bytebuffer_t* bytebuffer_merge(bytebuffer_t **buff_array, int nbuffers);
void bytebuffer_reset_reading(bytebuffer_t *s);
void bytebuffer_makeroom(bytebuffer_t *s, size_t size_to_add);

void bytebuffer_append_bytebuffer(bytebuffer_t *write_to,bytebuffer_t *write_from);
void bytebuffer_append_bulk(bytebuffer_t *s, void * start, size_t size);
#endif /* _BYTEBUFFER_H */
