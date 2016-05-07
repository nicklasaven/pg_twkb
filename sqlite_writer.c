

#include <math.h>
#include "pg_twkb.h"
#include "lwout_twkb.h"
#include "lwin_twkb.h"
#include "utils/builtins.h"
#include "executor/spi.h"

#include <sqlite3.h>


int create_sqlite_table(Portal *cur,sqlite3 *db);


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

int create_sqlite_table(Portal *cur,sqlite3 *db)
{
	char create_table_string[8192];
	char tmp_str[64];
	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	int i, rc;
	int strlengd = 0;
	char *err_msg, sqlitetype[15] ;
	
	/*Get fielads definition by ftching 0 rows*/
	SPI_cursor_fetch(*cur, true,0);
	

	snprintf(create_table_string, sizeof(create_table_string), " %s","create table test(");
	strlengd = strlen(create_table_string);
	tupdesc = SPI_tuptable->tupdesc;
	for (i = 1; i <= tupdesc->natts; i++)
	{
		elog(INFO, "pg_typ: %s\n", SPI_gettype(tupdesc, i));
		getsqlitetype(SPI_gettype(tupdesc, i), sqlitetype);
		snprintf(tmp_str, sizeof(tmp_str), " %s %s%s",
		SPI_fname(tupdesc, i),
		sqlitetype,
		(i == tupdesc->natts) ? " " : ", ");
		
		
		snprintf(create_table_string+strlengd, sizeof(create_table_string)-strlengd, " %s",tmp_str);
		
		strlengd += strlen(tmp_str);
	}
	snprintf(create_table_string+strlengd, sizeof(create_table_string)-strlengd, " %s",")");
	elog(INFO, "sql: %s", create_table_string);
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
	char *err_msg, sqlitetype[15] ;
	int spi_conn;
	int ret, proc, rc;
	/*Sqlite*/
	sqlite3 *db;
	sqlite3_stmt *res;
	TupleDesc tupdesc;
	SPITupleTable *tuptable;
HeapTuple tuple;
	int i, j;
	SPIPlanPtr plan;
	char buf[8192];
	Portal cur;
	char *our_cursor;
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

	create_sqlite_table(&cur,db);
	


	do
	{
		
		
		SPI_cursor_fetch(cur, true,10);
		
		proc = SPI_processed;
		
		 elog(INFO, "proc %d", proc);
//	    if (ret > 0 && SPI_tuptable != NULL)
//	    {
		    
		 tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		

		 elog(INFO, "proc %d", proc);
		for (j = 0; j < proc; j++)
		{
			
		 elog(INFO, "j: %d", j);
		    tuple = tuptable->vals[j];

		 elog(INFO, "test: %d", 19);
		 elog(INFO, "tupdesc->natts: %d", tupdesc->natts);
		    for (i = 1, buf[0] = 0; i <= tupdesc->natts; i++)
			{
		 elog(INFO, "i: %d", i);
			snprintf(buf + strlen (buf), sizeof(buf) - strlen(buf), " %s%s",
				SPI_getvalue(tuple, tupdesc, i),
				(i == tupdesc->natts) ? " " : " |");
		    elog(INFO, "EXECQ: %s", buf);
		}
	    
		}
	}
	while (proc > 0);
	SPI_finish();
	    sqlite3_close(db);
	    
return 0;
}














