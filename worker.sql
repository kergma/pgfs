
/*
Copyright (C) 2013-2015 Sergey Pushkin
https://github.com/kergma/pgfs
This file is part of pgfs
pgfs is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
pgfs is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with pgfs.  If not, see <http://www.gnu.org/licenses/>.
*/

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

create or replace function fs.file(_id int8,_depth int default null,_floor int default 0)
returns table (path int8[], id int8, md5 char(32), mime_type text, name text, size int, modified timestamp with time zone, data text, is_container boolean, expires timestamp with time zone, level int) language plpgsql as $_$
declare
	path int8[];
begin
	with
	recursive r as (
		select array[f.id]::int8[] as p,false as c, * from (select x as id,f.md5,f.mime_type,f.name,f.size,f.modified,f.container,f.is_container,f.expires from (values(_id)) as x(x) left join fs.files f on f.id=x) f
		union
		select f.id||p,f.id=any(p),f.* from  fs.files f
		join r on r.container=f.id
		where not c
	)
	select p[1:array_length(p,1)-1] from r order by array_length(p,1) desc limit 1
	into path;

	return query
	with recursive r as (
		select array[]::int8[] as o, array[f.id]::int8[] as p, false as c,* from (select x as id,f.md5,f.mime_type,f.name,f.size,f.modified,f.container,coalesce(f.is_container,true) as is_container,f.expires from (values(_id)) as x(x) left join fs.files f on f.id=x) f
		union
		select r.o||row_number() over (partition by r.p order by f.name,f.id), p||f.id,f.id=any(p),f.* from fs.files f 
		join r on r.id=f.container
		where not c
	)
	select path||p,r.id,r.md5,r.mime_type,r.name,r.size,r.modified,fs.data_path()||'/'||substr(r.md5,1,3)||'/'||r.md5,r.is_container,r.expires,cardinality(p)-_floor-1 from r
	where cardinality(p)-1>=_floor
	and cardinality(p)-_floor-2<coalesce(_depth,cardinality(p))
	order by o
	;
end
$_$;
		select array[f.id]::int8[] as p,false as c, * from (select x as id,f.md5,f.mime_type,f.name,f.size,f.modified,f.container,f.is_container,f.expires from (values(11)) as x(x) left join fs.files f on f.id=x) f
