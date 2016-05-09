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

#include <pg_twkb.h>

#include <math.h>
#include "pg_twkb.h"
#include "lwout_twkb.h"
#include "lwin_twkb.h"
#include "utils/builtins.h"
#include "executor/spi.h"

#include <sqlite3.h>

/*
 * This is required for builds against pgsql
 */
PG_MODULE_MAGIC;


PG_FUNCTION_INFO_V1(TWKBFromTWKBArray);
Datum TWKBFromTWKBArray(PG_FUNCTION_ARGS)
{
    ArrayType *arr_twkbs = NULL;
    ArrayType *arr_ids = NULL;
    int num_twkbs, num_ids, i = 0;

    ArrayIterator iter_twkbs, iter_ids;
    bool null_twkb, null_id;
    Datum val_twkb, val_id;

    uint8_t **col = NULL;
    size_t *sizes = NULL;
    int64_t *idlist = NULL;

    uint8_t *twkb;
    size_t twkb_size;
    size_t out_size;
    bytea *result;
    bytea *bytea_twkb;
    /* The first two arguments are required */
    if ( PG_NARGS() < 2 || PG_ARGISNULL(0) || PG_ARGISNULL(1) )
        PG_RETURN_NULL();

    arr_twkbs = PG_GETARG_ARRAYTYPE_P(0);
    arr_ids = PG_GETARG_ARRAYTYPE_P(1);

    num_twkbs = ArrayGetNItems(ARR_NDIM(arr_twkbs), ARR_DIMS(arr_twkbs));
    num_ids = ArrayGetNItems(ARR_NDIM(arr_ids), ARR_DIMS(arr_ids));

    if ( num_twkbs != num_ids )
    {
        elog(ERROR, "size of geometry[] and integer[] arrays must match");
        PG_RETURN_NULL();
    }

    /* Loop through array and build a collection of geometry and */
    /* a simple array of ids. If either side is NULL, skip it */

#if POSTGIS_PGSQL_VERSION >= 95
    iter_twkbs = array_create_iterator(arr_twkbs, 0, NULL);
    iter_ids = array_create_iterator(arr_ids, 0, NULL);
#else
    iter_twkbs = array_create_iterator(arr_twkbs, 0);
    iter_ids = array_create_iterator(arr_ids, 0);
#endif
    /* Construct collection/idlist first time through */
    col = palloc0(num_twkbs * sizeof(void*));
    sizes = palloc0(num_twkbs * sizeof(size_t));
    idlist = palloc0(num_twkbs * sizeof(int64_t));

    while( array_iterate(iter_twkbs, &val_twkb, &null_twkb) &&
            array_iterate(iter_ids, &val_id, &null_id) )
    {
        int32_t uid;

        if ( null_twkb || null_id )
        {
            elog(NOTICE, "ST_CollectTWKB skipping NULL entry at position %d", i);
            continue;
        }

        bytea_twkb =(bytea*) DatumGetPointer(val_twkb);
        uid = DatumGetInt64(val_id);



        twkb_size=VARSIZE_ANY_EXHDR(bytea_twkb);

        /* Store the values */
        col[i] = (uint8_t*)VARDATA(bytea_twkb);
        sizes[i] = twkb_size;
        idlist[i] = uid;
        i++;

    }

    array_free_iterator(iter_twkbs);
    array_free_iterator(iter_ids);


    if(i==0)
    {
        elog(NOTICE, "No valid geometry - id pairs found");
        PG_FREE_IF_COPY(arr_twkbs, 0);
        PG_FREE_IF_COPY(arr_ids, 1);
        PG_RETURN_NULL();
    }

    /* Write out the TWKB */
    twkb = twkb_to_twkbcoll(col, sizes,&out_size, idlist, i);

    /* Convert to a bytea return type */
    result = palloc(out_size + VARHDRSZ);
    memcpy(VARDATA(result), twkb,out_size);
    SET_VARSIZE(result, out_size + VARHDRSZ);

    /* Clean up */
    //~ pfree(twkb);
    pfree(idlist);
    PG_FREE_IF_COPY(arr_twkbs, 0);
    PG_FREE_IF_COPY(arr_ids, 1);

    PG_RETURN_BYTEA_P(result);

}




PG_FUNCTION_INFO_V1(TWKB2file);
Datum TWKB2file(PG_FUNCTION_ARGS)
{
    text *filename;
    bytea *bytea_twkb;
    uint8_t *twkb;
    FILE * file_p;

    if( PG_NARGS() < 1 || PG_ARGISNULL(0))
    {
        lwnotice("No buffer to write");
        PG_RETURN_NULL();
    }
    bytea_twkb = (bytea*)PG_GETARG_BYTEA_P(0);
    if ( PG_NARGS() < 2 || PG_ARGISNULL(1) )
    {
        lwerror("No filename to write to");
        PG_RETURN_NULL();
    }
    filename =  PG_GETARG_TEXT_P(1);


    twkb = (uint8_t*)VARDATA(bytea_twkb);

    file_p = fopen(text_to_cstring(filename), "ab");
    if (file_p == NULL)
        lwerror("Couldn't open the file for writing");

    fwrite(twkb, VARSIZE(bytea_twkb)-VARHDRSZ, 1, file_p);

    fclose(file_p);

    PG_FREE_IF_COPY(bytea_twkb, 0);
    PG_RETURN_INT32(1);
}


PG_FUNCTION_INFO_V1(TWKB_Write2SQLite);
Datum TWKB_Write2SQLite(PG_FUNCTION_ARGS)
{
    char *sqlitedb_name,*sql_string;

    if( PG_NARGS() < 1 || PG_ARGISNULL(0))
    {
        lwnotice("No sql query to use");
        PG_RETURN_NULL();
    }
    sql_string = text_to_cstring(PG_GETARG_TEXT_P(0));

    if ( PG_NARGS() < 2 || PG_ARGISNULL(1) )
    {
        lwerror("No sqlitedb to write to");
        PG_RETURN_NULL();
    }
    sqlitedb_name =  text_to_cstring(PG_GETARG_TEXT_P(1));

//	PG_FREE_IF_COPY(bytea_twkb, 0);
    write2sqlite(sql_string, sqlitedb_name);
    pfree(sqlitedb_name);
    pfree(sql_string);
    PG_RETURN_INT32(1);
}

PG_FUNCTION_INFO_V1(text2file);
Datum text2file(PG_FUNCTION_ARGS)
{
    text *filename;
    text *the_text;
    char *text_data;

    FILE * file_p;

    if( PG_NARGS() < 1 || PG_ARGISNULL(0))
    {
        lwnotice("No text to write");
        PG_RETURN_NULL();
    }
    the_text = PG_GETARG_TEXT_P(0);
    if ( PG_NARGS() < 2 || PG_ARGISNULL(1) )
    {
        lwerror("No filename to write to");
        PG_RETURN_NULL();
    }
    filename =  PG_GETARG_TEXT_P(1);


    text_data = (char*)VARDATA(the_text);

    file_p = fopen(text_to_cstring(filename), "w");
    if (file_p == NULL)
        lwerror("Couldn't open the file for writing");

    fwrite(text_data, VARSIZE(the_text)-VARHDRSZ, 1, file_p);

    fclose(file_p);

    PG_FREE_IF_COPY(the_text, 0);
    PG_RETURN_INT32(1);
}


PG_FUNCTION_INFO_V1(get_tileid);
Datum get_tileid(PG_FUNCTION_ARGS)
{

    int x, y,i, res=0;

    if( PG_NARGS() < 1 || PG_ARGISNULL(0))
    {
        lwnotice("No text to write");
        PG_RETURN_NULL();
    }
    x = PG_GETARG_INT32(0);
    if ( PG_NARGS() < 2 || PG_ARGISNULL(1) )
    {
        lwerror("No filename to write to");
        PG_RETURN_NULL();
    }
    y =  PG_GETARG_INT32(1);


    for (i=15; i>=0; i--)
    {
        res=res | ((1&(x>>i))<<2*i);
        res=res | ((1&(y>>i))<<(2*i+1));
    }



    PG_RETURN_INT32(res);
}

