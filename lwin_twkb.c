/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 *
 *
 * Copyright (C) 2015 Paul Ramsey
 * Copyright (C) 2014 Nicklas Avén
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the LICENCE file.
 **********************************************************************/


#include "lwin_twkb.h"

/**********************************************************************/

/**
* Check that we are not about to read off the end of the WKB
* array.
*/



static uint32_t lwtype_from_twkb_type(uint8_t twkb_type)
{
    switch (twkb_type)
    {
    case 1:
        return POINTTYPE;
    case 2:
        return LINETYPE;
    case 3:
        return POLYGONTYPE;
    case 4:
        return MULTIPOINTTYPE;
    case 5:
        return MULTILINETYPE;
    case 6:
        return MULTIPOLYGONTYPE;
    case 7:
        return COLLECTIONTYPE;

    default: /* Error! */
        //~ lwerror("Unknown WKB type");
        return 0;
    }
    return 0;
}

/**
* Byte
* Read a byte and advance the parse state forward.
*/
static uint8_t byte_from_twkb_state(twkb_parse_state *s)
{
    uint8_t val = *(s->pos);
    twkb_parse_state_advance(s, WKB_BYTE_SIZE);
    return val;
}


void header_from_twkb_state(twkb_parse_state *s)
{
    //~ LWDEBUG(2,"Entering magicbyte_from_twkb_state");

    uint8_t extended_dims;

    /* Read the first two bytes */
    uint8_t type_precision = byte_from_twkb_state(s);
    uint8_t metadata = byte_from_twkb_state(s);

    /* Strip type and precision out of first byte */
    uint8_t type = type_precision & 0x0F;
    int8_t precision = unzigzag8((type_precision & 0xF0) >> 4);

    /* Convert TWKB type to internal type */
    s->lwtype = lwtype_from_twkb_type(type);

    /* Convert the precision into factor */
    s->factor = pow(10, (double)precision);

    /* Strip metadata flags out of second byte */
    s->has_bbox   =  metadata & 0x01;
    s->has_size   = (metadata & 0x02) >> 1;
    s->has_idlist = (metadata & 0x04) >> 2;
    extended_dims = (metadata & 0x08) >> 3;
    s->is_empty   = (metadata & 0x10) >> 4;

    /* Flag for higher dims means read a third byte */
    if ( extended_dims )
    {
        int8_t precision_z, precision_m;

        extended_dims = byte_from_twkb_state(s);

        /* Strip Z/M presence and precision from ext byte */
        s->has_z    = (extended_dims & 0x01);
        s->has_m    = (extended_dims & 0x02) >> 1;
        precision_z = (extended_dims & 0x1C) >> 2;
        precision_m = (extended_dims & 0xE0) >> 5;

        /* Convert the precision into factor */
        s->factor_z = pow(10, (double)precision_z);
        s->factor_m = pow(10, (double)precision_m);
    }
    else
    {
        s->has_z = 0;
        s->has_m = 0;
        s->factor_z = 0;
        s->factor_m = 0;
    }

    /* Read the size, if there is one */
    if ( s->has_size )
    {
        s->size = twkb_parse_state_uvarint(s);
    }

    /* Calculate the number of dimensions */
    s->ndims = 2 + s->has_z + s->has_m;

    return;
}


