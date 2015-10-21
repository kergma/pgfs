/* vim: set ft=perl: */
create or replace function fs.store_file(_file json)
returns int8 language plperlu as $_$
	use strict;	
	use JSON 'from_json';
	use Cwd qw'abs_path cwd';
	use Digest::MD5;
	use File::MimeInfo::Magic;
	use File::Basename;
	use Encode;
	use Date::Format;

	our $f=from_json(shift//'{}');

	our $fsdata;
	if ($f->{file})
	{
		$fsdata=spi_exec_query("select fs.data_path()");
		$fsdata=$fsdata->{rows}[0]->{data_path};
		elog(ERROR,"$fsdata/buffer/ does not exist") unless -d "$fsdata/buffer";
		my $cwd=cwd();
		chdir "$fsdata/buffer";
		$f->{buffer_file}=abs_path("$f->{file}");
		chdir $cwd;
	};

	elog(ERROR,"file $f->{file} is not inside $fsdata/buffer") if $f->{buffer_file} and $f->{buffer_file} !~ "^$fsdata/buffer/";
	elog(ERROR,"file $f->{buffer_file} does not exist") if $f->{buffer_file} and !-f $f->{buffer_file};

	if ($f->{buffer_file} and !$f->{md5})
	{
		open FILE, $f->{buffer_file};
		eval {$f->{md5}=Digest::MD5->new->addfile(*FILE)->hexdigest};
		elog(ERROR, "cannot get md5 of $f->{buffer_file}") unless $f->{md5};
		close FILE;
	};

	$f->{name}=decode("utf8",basename($f->{buffer_file})) if $f->{buffer_file} and not defined $f->{name};
	$f->{size}= -s $f->{buffer_file} if $f->{buffer_file} and not defined $f->{size};
	$f->{mime_type}=File::MimeInfo::Magic::mimetype($f->{buffer_file}) if $f->{buffer_file} and !$f->{mime_type};
	$f->{modified}=Date::Format::time2str('%Y-%m-%dT%H:%M:%S',(stat($f->{buffer_file}))[9]) if $f->{buffer_file} and !$f->{modified};

	$f->{md5prefix}=substr($f->{md5},0,3) if $f->{md5};
	$f->{data_file}="$fsdata/$f->{md5prefix}/$f->{md5}" if $f->{md5};
	$f->{mkdir}=mkdir "$fsdata/$f->{md5prefix}" if $f->{buffer_file} and $f->{data_file} and !-d "$fsdata/$f->{md5prefix}";
	chmod 0711,"$fsdata/$f->{md5prefix}" if $f->{mkdir};
	rollback_fo() and elog(ERROR,"could not create $fsdata/$f->{md5prefix}/") if $f->{buffer_file} and $f->{data_file} and !-d "$fsdata/$f->{md5prefix}";
	$f->{rename}=rename $f->{buffer_file}, $f->{data_file} if $f->{buffer_file} and !-f $f->{data_file};
	rollback_fo() and elog(ERROR,"could not move $f->{data_file} to $f->{buffer_file}") if $f->{buffer_file} and !-f $f->{data_file};
	rollback_fo() and elog(ERROR,"file $f->{data_file} should exist") if $f->{md5} and !$f->{file} and !-f $f->{data_file};
	
	$f->{expires}=quote_nullable($f->{expires}) if defined $f->{expires};
	$f->{expires}=sprintf("current_timestamp+%d*interval '1 second'", $f->{ttl}) if $f->{ttl};
	$f->{expires}='default' unless defined $f->{expires};

	my $query=sprintf("(coalesce(%s,generate_id()),%s,%s,%s,%s,%s,%s,%s)",quote_nullable($f->{id}),quote_nullable($f->{md5}),quote_nullable($f->{mime_type}),quote_nullable($f->{name}),quote_nullable($f->{size}),quote_nullable($f->{modified}),quote_nullable($f->{container}),$f->{expires});
	$query="insert into fs.files(id,md5,mime_type,name,size,modified,container,expires) values $query returning id";

	my $rv;
	eval {$rv=spi_exec_query($query)};
	rollback_fo() and elog(ERROR,$@) if $@;

	system "sudo", "-n", "/fsdata/sieze", $f->{data_file} if $f->{data_file};

	return $rv->{rows}->[0]->{id};

	sub rollback_fo
	{
		rename $f->{data_file}, $f->{buffer_file} if $f->{rename};
		rmdir "$fsdata/$f->{md5prefix}" if $f->{mkdir};
		return 1;
	}

$_$;
