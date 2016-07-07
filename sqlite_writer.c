

#include <math.h>
#include "pg_twkb.h"
#include "lwout_twkb.h"
#include "lwin_twkb.h"
#include "utils/builtins.h"
#include "executor/spi.h"

#include <sqlite3.h>

#define SQLSTRLEN 8192

int create_sqlite_table(Portal *cur,sqlite3 *db,char *insert_str, char *table_name, char *id_name);
int create_spatial_index(sqlite3 *db,char  *table_name, char *geom_name, char *id_name, char *sql_string);

/*Input a postgres type and get a sqlite type back
ANything but what is defined in types results as "text"*/
int getsqlitetype(char *pgtype, char *sqlitetype)
{
	int i;
	
	static const char* typer[7][2]={
{"bool", "integer"},
{"int2", "integer"},
{"int4", "integer"},
{"int8", "integer"},
{"float4", "real"},
{"float8", "real"},
{"bytea", "blob"}
};

for (i=0;i<7;i++)
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

int create_sqlite_table(Portal *cur,sqlite3 *db,char *insert_str, char *table_name, char *id_name)
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
	

	snprintf(create_table_string, sizeof(create_table_string), " %s%s%s","create table ",table_name,"(");
	strlengd = strlen(create_table_string);
	
	snprintf(insert_str,SQLSTRLEN, " %s%s%s","insert into " ,table_name,"(");
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
		snprintf(tmp_str, sizeof(tmp_str), " %s %s%s%s",
		field_name,
		sqlitetype,
		(strcmp(field_name, id_name)==0) ? " primary key " : " ",
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
		
//	elog(INFO, "strlength %d, temp: %s",strlengd_ins, insert_str);
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

int create_spatial_index(sqlite3 *db,char  *table_name, char * geom_name, char *id_name, char *sql_string)
{
	char sql_txt_pg[SQLSTRLEN];
	char sql_txt_sqlite[SQLSTRLEN];
	int rc;
	char *err_msg;
	SPIPlanPtr plan;
	sqlite3_stmt *prepared_statement;
	Portal cur;
	char *pg_type;
	int val_int, proc,j;
	float8 val_float;
	
	int64 val_int64;
	bool null_check;
		TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;
	int tot_rows = 0;
	
	snprintf(sql_txt_pg,sizeof(sql_txt_pg), " %s%s%s",
	"CREATE VIRTUAL TABLE ",
	table_name,
	"_idx_geom USING rtree(id,minX, maxX,minY, maxY)");
	
		rc = sqlite3_exec(db, sql_txt_pg, NULL, 0, &err_msg);
	    
	    if (rc != SQLITE_OK ) {	
		    sqlite3_free(err_msg);
		sqlite3_close(db);	
		    ereport(ERROR,  ( errmsg("Problem creating table: %s", err_msg)));
		//fprintf(stderr, "SQL error: %s\n", err_msg);		
	    } 	
	   elog(INFO, "create table string: %s", sql_txt_pg); 
	    
	snprintf(sql_txt_pg,sizeof(sql_txt_pg), " %s%s%s%s%s%s%s%s%s",
	"with o as (",
	    sql_string,
	    "), g as( select ",
	id_name,
	" id,", 
	geom_name,
	" geom from ",
	    table_name,
	    ") select g.id, st_xmin(g.geom) minx,st_xmax(g.geom) maxx,st_ymin(g.geom) miny,st_ymax(g.geom) maxy from g inner join o on g.id=o.id");

	   elog(INFO, "select table string: %s", sql_txt_pg); 
	plan =  SPI_prepare(sql_txt_pg,0,NULL);
	//ret = SPI_exec(sql_string, 0);
	cur = SPI_cursor_open("index_cursor", plan,NULL,NULL,true);

	snprintf(sql_txt_sqlite,sizeof(sql_txt_sqlite), " %s%s%s",
	"insert into ",
	table_name,
	"_idx_geom  (id,minX, maxX,minY, maxY) values(?,?,?,?,?)"); 
  
  
	   elog(INFO, "insert table string: %s", sql_txt_sqlite); 
	    
	     sqlite3_prepare_v2(db,sql_txt_sqlite,strlen(sql_txt_sqlite), &prepared_statement,NULL);
	     
	     
	do
	{
		
	sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &err_msg);
		
		SPI_cursor_fetch(cur, true,100000);
		
		proc = SPI_processed;
		tot_rows += proc;
//	    if (ret > 0 && SPI_tuptable != NULL)
//	    {
		    
		 tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		

		for (j = 0; j < proc; j++)
		{
			
		    tuple = tuptable->vals[j];

				pg_type = SPI_gettype(tupdesc, 1);
				

				if(strcmp(pg_type, "int2")==0)
				{			
					val_int = (int) DatumGetInt16(SPI_getbinval(tuple,tupdesc,1, &null_check));
					//TODO add error handling
					if(null_check)
						sqlite3_bind_null(prepared_statement, 1);
					else
					sqlite3_bind_int(prepared_statement, 1,val_int);
				}
				else if(strcmp(pg_type, "int4")==0)
				{			
					val_int = (int) DatumGetInt32(SPI_getbinval(tuple,tupdesc,1, &null_check));
					//TODO add error handling
					if(null_check)
						sqlite3_bind_null(prepared_statement, 1);
					else
					sqlite3_bind_int(prepared_statement, 1,val_int);
					
				}	
				else if(strcmp(pg_type, "int8")==0)
				{			
					val_int64 = (int64) DatumGetInt64(SPI_getbinval(tuple,tupdesc,1, &null_check));
					//TODO add error handling
					if(null_check)
						sqlite3_bind_null(prepared_statement, 1);
					else
					sqlite3_bind_int64(prepared_statement,1,val_int64);
				}	



	
					val_float = (float8) DatumGetFloat8(SPI_getbinval(tuple,tupdesc,2, &null_check));
					//TODO add error handling
					if(null_check)
						sqlite3_bind_null(prepared_statement, 2);
					else
					sqlite3_bind_double(prepared_statement,2,val_float);
				
					val_float = (float8) DatumGetFloat8(SPI_getbinval(tuple,tupdesc,3, &null_check));
					//TODO add error handling
					if(null_check)
						sqlite3_bind_null(prepared_statement, 3);
					else
					sqlite3_bind_double(prepared_statement,3,val_float);
				
					val_float = (float8) DatumGetFloat8(SPI_getbinval(tuple,tupdesc,4, &null_check));
					//TODO add error handling
					if(null_check)
						sqlite3_bind_null(prepared_statement, 4);
					else
					sqlite3_bind_double(prepared_statement,4,val_float);
				
					val_float = (float8) DatumGetFloat8(SPI_getbinval(tuple,tupdesc,5, &null_check));
					//TODO add error handling
					if(null_check)
						sqlite3_bind_null(prepared_statement, 5);
					else
					sqlite3_bind_double(prepared_statement,5,val_float);
				
					
					
					
		
	    	sqlite3_step(prepared_statement);	
		sqlite3_clear_bindings(prepared_statement);
				sqlite3_reset(prepared_statement);
		}
		
	sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &err_msg);
	elog(INFO, "inserted %d rows in index",tot_rows);
	}
	while (proc > 0);
	   
	    
	    
	    
	return 0;
}

int write2sqlite(char* sql_string,char* table_name,char* geom_name,char* id_name, char* sqlitedb_name)
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
	int64 val_int64;
	float8 val_float;
	bool null_check;
	char *pg_type;	
	int tot_rows = 0;
	
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

	
	
	create_sqlite_table(&cur,db, insert_str,table_name, id_name);
	
	   elog(INFO, "back from creating table"); 
	
//TODO add error handling	
 sqlite3_prepare_v2(db,insert_str,strlen(insert_str), &prepared_statement,NULL);

	do
	{
		
	sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &err_msg);
		
		SPI_cursor_fetch(cur, true,10000);
		
		proc = SPI_processed;
		
		tot_rows += proc;
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
		
	elog(INFO, "inserted %d rows in table",tot_rows);
	}
	while (proc > 0);
	
	if(table_name && geom_name && id_name)
		create_spatial_index(db,table_name, geom_name, id_name, sql_string);
	else
		elog(INFO, "Finnishing without spatial index");
	
	SPI_finish();
	    sqlite3_close(db);
	    
return 0;
}














