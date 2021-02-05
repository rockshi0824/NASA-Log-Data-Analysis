--Rock Shi NASA Log Data Analysis

create database if not exists nasaDB;

use nasaDB;

drop table if exists nasa;

create external table if not exists nasa(
	source_ip string, 
	time_stamp string, 
	http_method string, 
	request_url string, 
	http_protocol string, 
	status_code string, 
	response_bytes string
) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' 
WITH SERDEPROPERTIES (
'input.regex'='(^.*) - - \\[(.*)\\] \\\"([A-Z]*|[^\\\" ]*) ([^HTTP]*) ([^\\\"]*)\\\" ([\\d]+) ([^-].*)', 
"output.format.string" = "%1$s %2$s %3$s %4$s %5$s %6$s %7$s" );

load data local inpath '/home/hadoop/Downloads/access_log_Aug95'
overwrite into table nasa;

select count(*) from nasa
where status_code = '200';

select count(distinct source_ip) from nasa;

select request_url, count(*) as num_requested from nasa
group by request_url
sort by num_requested desc
limit 1;

INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/nasa_requests' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select day(FROM_UNIXTIME(UNIX_TIMESTAMP(time_stamp, "dd/MMM/yyyy:HH:mm:ss ZZZZ"))), count(*) from nasa
where month(FROM_UNIXTIME(UNIX_TIMESTAMP(time_stamp, "dd/MMM/yyyy:HH:mm:ss ZZZZ"))) = 8
group by day(FROM_UNIXTIME(UNIX_TIMESTAMP(time_stamp, "dd/MMM/yyyy:HH:mm:ss ZZZZ")));
