/**********************************************************************
 *
 * pg_twkb - Spatial Types for PostgreSQL
 *
 * Copyright (C) 2014 Sandro Santilli <strk@keybit.net>
 * Copyright (C) 2013 Nicklas Avén
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the LICENCE file.
 *
 **********************************************************************
 *
 * Handle varInt values, as described here:
 * http://developers.google.com/protocol-buffers/docs/encoding#varints
 *
 **********************************************************************/

#ifndef _LIBLWGEOM_VARINT_H
#define _LIBLWGEOM_VARINT_H 1

#include "pg_twkb.h"

#define WKB_BYTE_SIZE 1

/* NEW SIGNATURES */

size_t varint_u64_encode_buf(uint64_t val, uint8_t *buf);
size_t varint_s64_encode_buf(int64_t val, uint8_t *buf);
int64_t varint_s64_decode(const uint8_t *the_start, const uint8_t *the_end, size_t *size);
uint64_t varint_u64_decode(const uint8_t *the_start, const uint8_t *the_end, size_t *size);

size_t varint_size(const uint8_t *the_start, const uint8_t *the_end);

uint64_t zigzag64(int64_t val);
uint8_t zigzag8(int8_t val);
int64_t unzigzag64(uint64_t val);
int8_t unzigzag8(uint8_t val);

#endif /* !defined _LIBLWGEOM_VARINT_H  */

