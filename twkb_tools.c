/**********************************************************************
 *
 * pg_twkb - Spatial Types for PostgreSQL
 *
 * Copyright (C) 2015 Nicklas Avén
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the LICENCE file.
 *
 **********************************************************************/

#include <pg_twkb.h>

#include <math.h>
#include "lwout_twkb.h"
#include "lwin_twkb.h"




static inline void twkb_parse_state_advance(twkb_parse_state *s, size_t next)
{
    if( (s->pos + next) > s->twkb_end)
    {
        //~ lwerror("%s: TWKB structure does not match expected size!", __func__);
        // lwnotice("TWKB structure does not match expected size!");
    }

    s->pos += next;
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


/**
* Calculates the size of the bbox in varints in the form:
* xmin, xdelta, ymin, ydelta
*/
size_t sizeof_bbox(TWKB_STATE *ts, int ndims)
{
    int i;
    uint8_t buf[16];
    size_t size = 0;
    //~ LWDEBUGF(2, "Entered %s", __func__);
    for ( i = 0; i < ndims; i++ )
    {
        size += varint_s64_encode_buf(ts->bbox_min[i], buf);
        size += varint_s64_encode_buf((ts->bbox_max[i] - ts->bbox_min[i]), buf);
    }
    return size;
}

static inline int64_t twkb_parse_state_varint(twkb_parse_state *s)
{
    size_t size;
    int64_t val = varint_s64_decode(s->pos, s->twkb_end, &size);
    twkb_parse_state_advance(s, size);
    return val;
}

static inline uint64_t twkb_parse_state_uvarint(twkb_parse_state *s)
{
    size_t size;
    uint64_t val = varint_u64_decode(s->pos, s->twkb_end, &size);
    twkb_parse_state_advance(s, size);
    return val;
}
double twkb_parse_state_double(twkb_parse_state *s, double factor)
{
    size_t size;
    int64_t val = varint_s64_decode(s->pos, s->twkb_end, &size);
    twkb_parse_state_advance(s, size);
    return val / factor;
}





typedef struct
{
    int64_t bbox_min[MAX_N_DIMS];
    int64_t bbox_max[MAX_N_DIMS];
    double min_factor;
    double min_factor_z;
    double min_factor_m;
    int has_z;
    int has_m;
    int is_empty;
} twkb_collection_data;


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




/**
* A function to collect many twkb-buffers into one collection
*/
static twkb_collection_data twkb_get_bboxes(uint8_t **twkb, size_t *sizes, int n)
{
    twkb_parse_state s;
    double min, max;
    int i;
    twkb_collection_data res;
    GBOX bbox;
    res.has_z = 0;
    res.has_m = 0;
    res.min_factor = res.min_factor_z = res.min_factor_m = 100000000;
    res.is_empty = 1;
    bbox.xmin = bbox.ymin = bbox.zmin = bbox.mmin = FLT_MAX;
    bbox.xmax = bbox.ymax = bbox.zmax = bbox.mmax = FLT_MIN;

    /*For each buffer, collect their bounding box*/
    for (i=0; i<n; i++)
    {
        /* Zero out the state */
        memset(&s, 0, sizeof(twkb_parse_state));
        /* Initialize the state appropriately */
        s.twkb = s.pos = twkb[i];
        s.twkb_end = twkb[i] + sizes[i];
        header_from_twkb_state(&s);

        if(!s.is_empty)
            res.is_empty = 0;
        if(!s.has_bbox)
            lwerror("We can only collect twkb with included bbox");

        /*read bbox and expand to all*/

        if(s.factor<res.min_factor)
            res.min_factor=s.factor;

        /* X */
        min = twkb_parse_state_double(&s, s.factor);
        if(min < bbox.xmin)
            bbox.xmin=min;
        max = min + twkb_parse_state_double(&s, s.factor);
        if(max > bbox.xmax)
            bbox.xmax=max;
        /* Y */
        min = twkb_parse_state_double(&s, s.factor);
        if(min < bbox.ymin)
            bbox.ymin=min;
        max = min + twkb_parse_state_double(&s, s.factor);
        if(max > bbox.ymax)
            bbox.ymax=max;
        /* Z */
        if ( s.has_z )
        {
            if(s.factor_z<res.min_factor_z)
                res.min_factor_z=s.factor_z;
            res.has_z= 1;
            min = twkb_parse_state_double(&s, s.factor_z);
            if(min < bbox.zmin)
                bbox.zmin=min;
            max = min + twkb_parse_state_double(&s, s.factor_z);
            if(max > bbox.zmax)
                bbox.zmax=max;
        }
        /* M */
        if ( s.has_m )
        {
            if(s.factor_m<res.min_factor_m)
                res.min_factor_m=s.factor_m;
            res.has_m= 1;
            min = twkb_parse_state_double(&s, s.factor_m);
            if(min < bbox.mmin)
                bbox.mmin=min;
            max = min + twkb_parse_state_double(&s, s.factor_m);
            if(max > bbox.mmax)
                bbox.mmax=max;
        }
    }

    res.bbox_min[0] = (int64_t) lround(bbox.xmin*res.min_factor);
    res.bbox_max[0] = (int64_t) lround(bbox.xmax*res.min_factor);
    res.bbox_min[1] = (int64_t) lround(bbox.ymin*res.min_factor);
    res.bbox_max[1] = (int64_t) lround(bbox.ymax*res.min_factor);
//	lwnotice("st_makebox2d(st_point(%d, %d),st_point(%d, %d))::geometry\n", res.bbox_min[0],res.bbox_min[1],res.bbox_max[0],res.bbox_max[1]);
    if ( res.has_z )
    {
        res.bbox_min[2] = (int64_t) lround(bbox.zmin*res.min_factor);
        res.bbox_max[2] = (int64_t) lround(bbox.zmax*res.min_factor);
    }
    if ( res.has_m )
    {
        res.bbox_min[2+res.has_z] = (int64_t) lround(bbox.mmin*res.min_factor);
        res.bbox_max[2+res.has_z] = (int64_t) lround(bbox.mmax*res.min_factor);
    }
    return res;
}



/**
* Writes the bbox in varints in the form:
* xmin, xdelta, ymin, ydelta
*/
void write_bbox(TWKB_STATE *ts, int ndims)
{
    int i;
    //~ LWDEBUGF(2, "Entered %s", __func__);
    for ( i = 0; i < ndims; i++ )
    {
        bytebuffer_append_varint(ts->header_buf, ts->bbox_min[i]);
        bytebuffer_append_varint(ts->header_buf, (ts->bbox_max[i] - ts->bbox_min[i]));
    }
}




static uint8_t* twkb_write_new_buffer(twkb_collection_data ts_data,uint8_t **twkb, size_t *sizes, size_t *out_size, int64_t *idlist, int n)
{
    TWKB_STATE ts;
    int i;
    uint8_t type_prec = 0;
    uint8_t flag = 0;
    size_t size_to_register=0;
    uint8_t *twkb_out;
    int n_dims;
    int has_extended = (int) (ts_data.has_z||ts_data.has_m);

    /*Ok, we have all bounding boxes, then it's time to write*/
    ts.header_buf = bytebuffer_create_with_size(16);
    ts.geom_buf = bytebuffer_create_with_size(32);

    /* The type will always be COLLECTIONTYPE */
    TYPE_PREC_SET_TYPE(type_prec,COLLECTIONTYPE);
    /* Precision here is only for bbox. We use the worst precission we have found */
    TYPE_PREC_SET_PREC(type_prec, zigzag8((int) log10(ts_data.min_factor)));
    /* Write the type and precision byte */
    bytebuffer_append_byte(ts.header_buf, type_prec);

    /* METADATA BYTE */
    /* Always bboxes */
    FIRST_BYTE_SET_BBOXES(flag, 1);
    /* Always sizes */
    FIRST_BYTE_SET_SIZES(flag,1);
    /* Is there idlist? */
    FIRST_BYTE_SET_IDLIST(flag, idlist && !ts_data.is_empty);
    /* No extended byte */
    FIRST_BYTE_SET_EXTENDED(flag, has_extended);
    /* Write the header byte */
    bytebuffer_append_byte(ts.header_buf, flag);

    if(has_extended)
    {
        uint8_t flag = 0;

        if ( ts_data.has_z && (log10(ts_data.min_factor_z) > 7 || log10(ts_data.min_factor_z) < 0 ) )
            lwerror("%s: Z precision cannot be negative or greater than 7", __func__);

        if ( ts_data.has_m && ( log10(ts_data.min_factor_m) > 7 || log10(ts_data.min_factor_m)) )
            lwerror("%s: M precision cannot be negative or greater than 7", __func__);

        HIGHER_DIM_SET_HASZ(flag, ts_data.has_z);
        HIGHER_DIM_SET_HASM(flag, ts_data.has_m);
        HIGHER_DIM_SET_PRECZ(flag,(int) log10(ts_data.min_factor_z));
        HIGHER_DIM_SET_PRECM(flag,(int) log10(ts_data.min_factor_m));
        bytebuffer_append_byte(ts.header_buf, flag);

    }

    /*Write number of geometries*/
    bytebuffer_append_uvarint(ts.geom_buf, n);

    /* We've been handed an idlist, so write it in */
    if ( idlist )
    {
        for ( i = 0; i < n; i++ )
        {
            bytebuffer_append_varint(ts.geom_buf, idlist[i]);
        }
    }

    n_dims = 2 + ts_data.has_z+ts_data.has_m;

    for ( i = 0; i < n_dims; i++ )
    {
        ts.bbox_min[i]=ts_data.bbox_min[i];
        ts.bbox_max[i]=ts_data.bbox_max[i];
    }
    for ( i = 0; i < n; i++ )
    {
        //lwnotice("n=%d and i = %d\n",n,i);
        size_to_register += sizes[i];
        //lwnotice("size = %d and size_tot=%d\n",sizes[i],size_to_register );
    }
    /*Write the size of our new collection from size-info to the end*/
    size_to_register += sizeof_bbox(&ts,n_dims) + bytebuffer_getlength(ts.geom_buf);
    bytebuffer_append_uvarint(ts.header_buf, size_to_register);

    /*Write the bbox to the buffer*/
    write_bbox(&ts, n_dims);

    /*Put the id-list after the header*/
    bytebuffer_append_bytebuffer(ts.header_buf,ts.geom_buf);

    /*Copy all ingoing buffers "as is" to the new buffer*/
    for ( i = 0; i < n; i++ )
    {
        bytebuffer_append_bulk(ts.header_buf, twkb[i], sizes[i]);
    }


    bytebuffer_destroy(ts.geom_buf);

    if ( out_size )
        *out_size = bytebuffer_getlength(ts.header_buf);

    twkb_out = ts.header_buf->buf_start;

    lwfree(ts.header_buf);

    return twkb_out;
}
/**
* A function to collect many twkb-buffers into one collection
*/
uint8_t* twkb_to_twkbcoll(uint8_t **twkb, size_t *sizes,size_t *out_size, int64_t *idlist, int n)
{

    twkb_collection_data ts_data = twkb_get_bboxes(twkb, sizes, n);

    return twkb_write_new_buffer(ts_data,twkb, sizes, out_size, idlist, n);
}

