
--create extension postgis_sfcgal

--drop view onepoly;

drop sequence tmp_sequence;

CREATE SEQUENCE tmp_sequence
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;

select count(*) from gsd_1000.mark

--select * from __trianglar

drop table if exists tmp.subgeoms;
create table tmp.subgeoms as
with a as (select kkod, gid orig_id, 
st_subdivide((st_dump(st_collectionextract(st_makevalid(st_removerepeatedpoints(st_snaptogrid(st_simplifyvw(geom, 0.5),1),0)),3))).geom,2048) geom 
from gsd_1000.mark
--where 
--geom && st_makeline(st_point(360001,6600001),st_point(365000,6605000)) 
--id in (123511,123649 , 123389,123645)
--id in (123645,123649)
--ogc_fid in( 2712944,2699442,2638273)
)
--,e as (select kkod,id orig_id, (st_dumprings(geom)).geom geom from a)
,b as (select kkod, orig_id, ST_ForceRHR((st_dump(st_collectionextract(st_makevalid(geom),3))).geom) geom from a)
select * from b;
alter table tmp.subgeoms add column id serial primary key;

select 'klart med steg 1'::text;
--select count(*) from __subgeoms

drop table if exists tmp.trianglar;
create /*temp*/ table tmp.trianglar as
with a as (select id, (st_dump(st_tesselate(geom))).geom tri,nextval('tmp_sequence'::regclass) trid from tmp.subgeoms) 
,b as (select id, trid, st_dumppoints(tri) d from a)
,c as (select id, trid, (d).geom tri, (d).path from b where (d).path[2] < 4)
select * from c;
create index idx_tri_geom on tmp.trianglar using gist(tri);
create index idx_tri_id on tmp.trianglar using btree(id);
analyze tmp.trianglar;


select 'klart med triangulering'::text;

drop table if exists tmp.boundary;
create table tmp.boundary as
with a as (select kkod,id,orig_id,  st_dumprings(geom) d from tmp.subgeoms )
,b as (select kkod,id, orig_id, ST_ExteriorRing((d).geom) geom, (d).path path from a order by (d).path)
,c as (select st_npoints(geom) npoints, kkod,id, orig_id,geom, path from b)
select kkod,id, orig_id,st_collect(st_removepoint(geom,npoints-1) order by path) geom from c group by kkod,id, orig_id;


drop table if exists tmp.boundarypoints;
create table tmp.boundarypoints as
with a as (select id, st_dumppoints(geom) d from tmp.subgeoms)
, b as (select id, (d).geom geom, (d).path path from a)
, c as(select id, geom,path, max(path[2]) over (partition by id, path[1]) max_path from b)
, d as (select id, st_collect(geom order by path) geom from c where path[2] < max_path group by id) 
--, p as(insert into point_arrays(p_array, id) select st_astwkb(geom), id from d)
, e as(select id, st_dumppoints(geom) d from d)
select id, (d).geom geom, (d).path[1] point_index from e order by id, (d).path;



drop table if exists tmp.boundarypoints;
create table tmp.boundarypoints as
select id, (d).geom geom, (d).path[1] point_index from 
(
select id, st_dumppoints(geom) d from 
(
select id, st_collect(geom order by path) geom from
 (
select id, geom,path, max(path[2]) over (partition by id, path[1]) max_path from 
(
select id, (d).geom geom, (d).path path from 
(
select id, st_dumppoints(geom) d from tmp.subgeoms
) a
)b
) c where path[2] < max_path group by id
) d
) e order by id, (d).path;





drop table if exists tmp.triindex;
create table tmp.triindex as
select p.id,t.id tid, t.trid,  p.point_index - 1 point_index from tmp.boundarypoints p inner join  tmp.trianglar t on p.geom && t.tri and t.tri && p.geom limit 0;
 
insert into tmp.triindex(id, tid, trid, point_index)
select p.id,t.id tid, t.trid,  p.point_index - 1 point_index from tmp.boundarypoints p inner join  tmp.trianglar t on p.geom && t.tri and t.tri && p.geom;

delete from tmp.triindex where not id=tid;

/*
drop table if exists tmp.index_array;
create table tmp.index_array as
with a as(
select id, array_agg(point_index) t from tmp.triindex group by id, trid)
select id, st_astwkb(ST_MakeLine(st_makepoint(t[1], t[2], t[3]))) pi from a group by id;
*/


drop table if exists tmp.index_array_step0;

create table tmp.index_array_step0 as
select id, array_agg(point_index) t from tmp.triindex group by id, trid;

create index idx_ta0_id
on tmp.index_array_step0
using btree(id);
analyze tmp.index_array_step0;

drop table if exists tmp.index_array;

create table tmp.index_array as
select id, st_astwkb(ST_MakeLine(st_makepoint(t[1], t[2], t[3]))) pi from (select * from tmp.index_array_step0 order by id ) a group by id order by id limit 0;

insert into tmp.index_array(id, pi)
select id, st_astwkb(ST_MakeLine(st_makepoint(t[1], t[2], t[3]))) pi from (select * from tmp.index_array_step0 order by id ) a group by id order by id ;


--delete from tmp.index_array

create or replace temp view onepoly as
select * from tmp.boundary;

select * from tmp.index_array;

select TWKB_Write2SQLite('/tmp/sverige.sqlite',
'gsd_1000_mark',
'SELECT bd.kkod, bd.id, bd.orig_id, st_astwkb(geom) twkb,ia.pi tri_index from 
tmp.boundary bd inner join
tmp.index_array ia on bd.id=ia.id',
'twkb','id', 'tmp.boundary','geom', 'id',1);

/*
select sum(st_npoints(geom)) from onepoly

select * from __triindex order by trid
select * from __boundarypoints

drop table boundary;
create table boundary as
select * from __subgeoms


*/

--select st_npoints(geom) from __boundary
