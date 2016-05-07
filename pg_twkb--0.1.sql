-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_twkb" to load this file. \quit

   CREATE TYPE id_box AS
   (id integer,
    box geometry);
    
    
    
    CREATE OR REPLACE FUNCTION TWKB_Collect(twkb bytea[], ids bigint[])
 	        RETURNS bytea
 	        AS 'MODULE_PATHNAME','TWKBFromTWKBArray'
       LANGUAGE 'c' IMMUTABLE;
       
       
       
       
CREATE OR REPLACE FUNCTION TWKB_Write2File(twkb bytea,filename text)
  RETURNS void 
  AS 'MODULE_PATHNAME', 'TWKB2file'
  LANGUAGE c IMMUTABLE;
  
  CREATE OR REPLACE FUNCTION TWKB_Write2File( the_text text,filename text)
  RETURNS void 
  AS 'MODULE_PATHNAME', 'text2file'
  LANGUAGE c IMMUTABLE; 
  
       
CREATE OR REPLACE FUNCTION TWKB_Write2SQLite(sql_string text,sqlitedb text)
  RETURNS void 
  AS 'MODULE_PATHNAME', 'TWKB_Write2SQLite'
  LANGUAGE c IMMUTABLE;
  
       
CREATE OR REPLACE FUNCTION TWKB_getTileId(x int, y int)
  RETURNS int 
  AS 'MODULE_PATHNAME', 'get_tileid'
  LANGUAGE c IMMUTABLE;
  
    
  
CREATE OR REPLACE FUNCTION TWKB_MakeSquarebox(
    table_name text,
    geomcol text)
  RETURNS box2d AS
$BODY$
declare 
the_box box2d;
begin
execute
'with orig as
(select st_extent('||geomCol||') b from '||table_name::regclass||'),
deltaCenter as (select st_xmax(b)-st_xmin(b) deltax,st_xmin(b) + (st_xmax(b)-st_xmin(b))/2 xcenter,st_ymax(b)-st_ymin(b) deltay,st_ymin(b) + (st_ymax(b)-st_ymin(b))/2 ycenter from orig)
select st_makebox2d(st_point(xcenter-(greatest(deltax, deltay)/2),ycenter-(greatest(deltax, deltay)/2)),st_point(xcenter+(greatest(deltax, deltay)/2),ycenter+(greatest(deltax, deltay))/2)) from deltacenter;'	
into the_box;
return the_box;	
end
$BODY$
  LANGUAGE plpgsql VOLATILE;



CREATE OR REPLACE FUNCTION TWKB_DivideBox(
    box geometry,
    n_x integer,
    n_y integer,
    parent_id bigint)
  RETURNS SETOF id_box AS
$BODY$
DECLARE
ib id_box;
the_box geometry;
partofx float;
partofy float;
start_x float;
start_y float;
end_x float;
end_y float;
deltax float;
deltay float;
BEGIN
start_x=st_xmin(box);
start_y=st_ymin(box);
end_x=st_xmax(box);
end_y=st_ymax(box);
deltax=end_x-start_x;
deltay=end_y-start_y;
partofx = (deltax)/n_x;
partofy = (deltay)/n_y;

FOR i in 0..n_x-1 loop
	FOR j in 0..n_y-1 loop
		ib.box = st_envelope(ST_COLLECT(st_point(start_x+i*partofx,start_y+j*partofy),st_point(start_x+(i+1)*partofx,start_y+(j+1)*partofy) ));
		ib.id=i*n_x+j;
	RETURN NEXT ib;
	end loop;
end loop;
    RETURN;
END;
$BODY$
  LANGUAGE plpgsql IMMUTABLE;



CREATE OR REPLACE FUNCTION _TWKB_IndexedTiles(
    table_name text,
    geom_column text,
    id_column text,
    n_decimals integer,
    in_box geometry,
    srid_box integer,
    points_per_tile integer,
    parent_id bigint)
  RETURNS bytea AS
$BODY$
declare res bytea;
begin
execute
'with 
the_box as	(
		select (ib).id id,st_setsrid((ib).box,$1) box
		from (select TWKB_DivideBox( $2,2,2,$8 ) ib) s
	)
,id_geometries as(
		select b.id, ST_ClipByBox2D(geom,box) geom,gid 
		from (select '
		|| id_column ||
		' gid, '
		|| geom_column ||' geom  from '
		|| table_name::regclass ||
		') s,  the_box b
		where geom&&box
	)
, nr_check as (
		select sum(st_npoints(geom)) antal, id 
		from id_geometries group by id 
	)
,the_magic as (
		select _TWKB_IndexedTiles($3, $4,$5,$6, b.box, $1, $7,b.id) res, b.id from the_box b inner join nr_check nr on b.id=nr.id where antal >$7
		union all
select st_astwkb(array_agg(geom),array_agg(gid),$6,0,0,true,true) res	,id from
	(select (st_dump(st_makevalid(geom))).geom geom, gid, a.id  
		from (select id,gid,(st_dump(geom)).geom geom from id_geometries ) a inner join nr_check nr on a.id=nr.id where not st_isempty(geom) and 
antal <=$7 and antal > 0 ) v
group by id
	)
 select twkb_collect(array_agg(res), array_agg(id)) 
 from (SELECT * FROM the_magic)g;'
into res
 using  srid_box, in_box, table_name, geom_column, id_column,n_decimals, points_per_tile,parent_id::int8;
return res;
end
$BODY$
  LANGUAGE plpgsql VOLATILE;
  
  
CREATE OR REPLACE FUNCTION TWKB_IndexedTiles(
    table_name text,
    geom_column text,
    id_column text,
    n_decimals integer,
    srid_in integer,
    points_per_tile integer)
  RETURNS bytea AS
$BODY$
select _TWKB_IndexedTiles(table_name, geom_column,id_column,n_decimals, TWKB_MakeSquarebox(table_name, geom_column)::geometry,srid_in,points_per_tile,0);
$BODY$
  LANGUAGE sql VOLATILE;


