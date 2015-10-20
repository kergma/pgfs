drop schema fs cascade;
create schema fs;

create table fs.files (
	id int8 default generate_id() primary key,
	md5 char(32),
	mime_type text,
	name text,
	size int,
	modified timestamp with time zone,
	container int8,
	is_container boolean,
	expires timestamp with time zone
);

select * from create_indexes('fs.files');

create or replace function fs.data_path()
returns text language sql immutable
as $_$
	select '/fsdata'::text;
$_$;

create or replace function fs.container_add(_container int8, _items int8[])
returns int8 language sql
as $_$
	update fs.files set container=_container where id=any(_items);
	select _container;
$_$;

create or replace function fs.container_add(_container int8, _item int8)
returns int8 language sql
as $_$
	update fs.files set container=_container where id=_item returning container;
$_$;

create or replace function fs.file(_id int8)
returns table (path int8[], id int8, md5 char(32), mime_type text, name text, size int, modified timestamp with time zone, data text, is_container boolean, expires timestamp with time zone) language plpgsql as $_$
declare
	path int8[];
begin
	with
	recursive r as (
		select array[f.id]::int8[] as p,false as c, * from fs.files f where f.id=_id
		union
		select f.id||p,f.id=any(p),f.* from  fs.files f
		join r on r.container=f.id
		where not c
	)
	select p[1:array_length(p,1)-1] from r order by array_length(p,1) desc limit 1
	into path;

	return query
	with recursive r as (
		select array[]::int8[] as o, array[f.id]::int8[] as p, false as c,* from fs.files f where f.id=_id
		union
		select r.o||row_number() over (partition by r.p order by f.name,f.id), p||f.id,f.id=any(p),f.* from fs.files f 
		join r on r.id=f.container
		where not c
	)
	select path||p,r.id,r.md5,r.mime_type,r.name,r.size,r.modified,fs.data_path()||'/'||substr(r.md5,1,3)||'/'||r.md5,r.is_container,r.expires from r
	order by o
	;
end
$_$;