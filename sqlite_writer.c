

#include <math.h>
#include "pg_twkb.h"
#include "lwout_twkb.h"
#include "lwin_twkb.h"
#include "utils/builtins.h"
#include "executor/spi.h"

#include <sqlite3.h>

#define SQLSTRLEN 8192

int create_sqlite_table(Portal *cur,sqlite3 *db,char *insert_str);


/*Input a postgres type and get a sqlite type back
ANything but what is defined in types results as "text"*/
int getsqlitetype(char *pgtype, char *sqlitetype)
{
    int i;

    static const char* typer[7][2]= {
        {"bool", "integer"},
        {"int2", "integer"},
        {"int4", "integer"},
        {"int8", "integer"},
        {"float4", "real"},
        {"float8", "real"},
        {"bytea", "blob"}
    };

    for (i=0; i<7; i++)
    {
        if(strcmp(pgtype, (char*) typer[i][0])==0)
        {
            strncpy( sqlitetype,(char*) typer[i][1],15);
            return 0;
        }
    }

    strncpy(sqlitetype,"text",5);
    return 0;
}

int create_sqlite_table(Portal *cur,sqlite3 *db,char *insert_str)
{
    char create_table_string[SQLSTRLEN];
    char tmp_str[64];
    TupleDesc tupdesc;
    int i, rc;
    int strlengd = 0;
    int strlengd_ins = 0;
    char *err_msg, sqlitetype[15] ;
    char field_name[128];
    char value_list[256];
    int strlengd_vals = 0;
    /*Get fielads definition by fetching 0 rows*/
    SPI_cursor_fetch(*cur, true,0);


    snprintf(create_table_string, sizeof(create_table_string), " %s","create table test(");
    strlengd = strlen(create_table_string);

    snprintf(insert_str,SQLSTRLEN, " %s","insert into test(");
    strlengd_ins = strlen(insert_str);


    snprintf(value_list,sizeof(value_list), " %s","values(");
    strlengd_vals = strlen(value_list);

    tupdesc = SPI_tuptable->tupdesc;

    for (i = 1; i <= tupdesc->natts; i++)
    {
        snprintf(field_name, sizeof(field_name), "%s", SPI_fname(tupdesc, i));

        //convert type to sqlite type
        getsqlitetype(SPI_gettype(tupdesc, i), sqlitetype);

        //put together field name, type and comma sign if not last column
        snprintf(tmp_str, sizeof(tmp_str), " %s %s%s",
                 field_name,
                 sqlitetype,
                 (i == tupdesc->natts) ? " " : ", ");

        //put the column name and type in the create-table sql-string
        snprintf(create_table_string+strlengd, sizeof(create_table_string)-strlengd, " %s",tmp_str);
        strlengd += strlen(tmp_str);

        //construct the insert string with field names
        snprintf(insert_str+strlengd_ins, SQLSTRLEN-strlengd_ins, "%s%s",
                 field_name,
                 (i == tupdesc->natts) ? " " : ", ");
        strlengd_ins += strlen(field_name)+1; //adding 1 for the comma-sign

        //construct the value part of the insert
        snprintf(value_list+strlengd_vals, sizeof(value_list)-strlengd_vals, "%s%s",
                 "?",
                 (i == tupdesc->natts) ? " " : ", ");
        strlengd_vals += 2; //adding 1 for the comma-sign

        elog(INFO, "strlength %d, temp: %s",strlengd_ins, insert_str);
    }
    snprintf(create_table_string+strlengd, sizeof(create_table_string)-strlengd, " %s",")");


    elog(INFO, " SQLSTRLEN-strlengd_ins: %d, insert sql: %s", SQLSTRLEN-strlengd_ins, insert_str);
//	snprintf(insert_str+strlengd_ins, SQLSTRLEN-strlengd_ins, " %s",")");


    snprintf(insert_str+strlengd_ins, SQLSTRLEN-strlengd_ins, " %s%s%s",")",value_list,")" );

    elog(INFO, "sql: %s", create_table_string);
    elog(INFO, "insert sql: %s", insert_str);
    rc = sqlite3_exec(db, create_table_string, NULL, 0, &err_msg);

    if (rc != SQLITE_OK ) {
        sqlite3_free(err_msg);
        sqlite3_close(db);
        ereport(ERROR,  ( errmsg("Problem creating table: %s", err_msg)));
        //fprintf(stderr, "SQL error: %s\n", err_msg);

    }
    return 0;
}



int write2sqlite(char* sql_string, char* sqlitedb_name)
{
    char *err_msg;
    int spi_conn;
    int  proc, rc;
    /*Sqlite*/
    sqlite3 *db;
    TupleDesc tupdesc;
    SPITupleTable *tuptable;
    HeapTuple tuple;
    int i, j;
    SPIPlanPtr plan;
    char insert_str[SQLSTRLEN];
    Portal cur;
    void *val_p;
    int val_int;
    int val_int64;
    float8 val_float;
    bool null_check;
    char *pg_type;

    sqlite3_stmt *prepared_statement;

    spi_conn = SPI_connect();
    if (spi_conn!=SPI_OK_CONNECT)
        ereport(ERROR,  ( errmsg("Failed to open SPI Connection")));

    /*Open the sqlite db to write to*/
    rc = sqlite3_open(sqlitedb_name, &db);


    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        ereport(ERROR,  ( errmsg("Cannot open SQLite database")));
    }

    plan =  SPI_prepare(sql_string,0,NULL);
    //ret = SPI_exec(sql_string, 0);
    cur = SPI_cursor_open("our_cursor", plan,NULL,NULL,true);



    create_sqlite_table(&cur,db, insert_str);

//TODO add error handling
    sqlite3_prepare_v2(db,insert_str,strlen(insert_str), &prepared_statement,NULL);

    do
    {

        sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &err_msg);

        SPI_cursor_fetch(cur, true,10000);

        proc = SPI_processed;

//	    if (ret > 0 && SPI_tuptable != NULL)
//	    {

        tupdesc = SPI_tuptable->tupdesc;
        tuptable = SPI_tuptable;


        for (j = 0; j < proc; j++)
        {

            tuple = tuptable->vals[j];


            for (i = 1; i <= tupdesc->natts; i++)
            {
                pg_type = SPI_gettype(tupdesc, i);
                if(strcmp(pg_type, "bool")==0)
                {
                    val_int = (bool) (DatumGetBool(SPI_getbinval(tuple,tupdesc,i, &null_check)) ? 1:0);
                    if(null_check)
                        sqlite3_bind_null(prepared_statement, i);
                    else
                        sqlite3_bind_int(prepared_statement, i,(int) val_int);
                }
                if(strcmp(pg_type, "int2")==0)
                {
                    val_int = (int) DatumGetInt16(SPI_getbinval(tuple,tupdesc,i, &null_check));
                    //TODO add error handling
                    if(null_check)
                        sqlite3_bind_null(prepared_statement, i);
                    else
                        sqlite3_bind_int(prepared_statement, i,val_int);
                }
                else if(strcmp(pg_type, "int4")==0)
                {
                    val_int = (int) DatumGetInt32(SPI_getbinval(tuple,tupdesc,i, &null_check));
                    //TODO add error handling
                    if(null_check)
                        sqlite3_bind_null(prepared_statement, i);
                    else
                        sqlite3_bind_int(prepared_statement, i,val_int);

                }
                else if(strcmp(pg_type, "int8")==0)
                {
                    val_int64 = (int64) DatumGetInt64(SPI_getbinval(tuple,tupdesc,i, &null_check));
                    //TODO add error handling
                    if(null_check)
                        sqlite3_bind_null(prepared_statement, i);
                    else
                        sqlite3_bind_int64(prepared_statement, i,val_int64);
                }
                else if(strcmp(pg_type, "float4")==0)
                {
                    val_float = (float8) DatumGetFloat4(SPI_getbinval(tuple,tupdesc,i, &null_check));
                    //TODO add error handling
                    if(null_check)
                        sqlite3_bind_null(prepared_statement, i);
                    else
                        sqlite3_bind_double(prepared_statement, i,val_float);

                }
                else if(strcmp(pg_type, "float8")==0)
                {
                    val_float = (float8) DatumGetFloat8(SPI_getbinval(tuple,tupdesc,i, &null_check));
                    //TODO add error handling
                    if(null_check)
                        sqlite3_bind_null(prepared_statement, i);
                    else
                        sqlite3_bind_double(prepared_statement, i,val_float);
                }
                else if(strcmp(pg_type, "bytea")==0)
                {
                    val_p = (void*) PG_DETOAST_DATUM(SPI_getbinval(tuple,tupdesc,i, &null_check));
                    //TODO add error handling
                    if(null_check)
                        sqlite3_bind_null(prepared_statement, i);
                    else
                        sqlite3_bind_blob(prepared_statement, i, (const void*) VARDATA_ANY(val_p), VARSIZE_ANY(val_p)-VARHDRSZ, SQLITE_TRANSIENT);
                }
                else
                {
                    //	val = (void*) PG_DETOAST_DATUM(SPI_getbinval(tuple,tupdesc,i, &null_check));
                    //TODO add error handling
                    sqlite3_bind_text(prepared_statement,i,SPI_getvalue(tuple, tupdesc, i),-1,NULL);
                }

            }
            sqlite3_step(prepared_statement);
            sqlite3_clear_bindings(prepared_statement);
            sqlite3_reset(prepared_statement);
        }

        sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &err_msg);
    }
    while (proc > 0);
    SPI_finish();
    sqlite3_close(db);

    return 0;
}














