create database if not exists music;
use music;
SET hive.auto.convert.join=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=non-strict;
create table if not exists enriched_data
( user_id string,
  song_id string,
  artist_id string,
  timestamp string,
  start_ts string,
  end_ts string,
  geo_cd string,
  station_id string,
  song_end_type int,
  like int,
  dislike int
)partitioned by (batchid int,status string,date string)
row format delimited 
fields terminated by',';

create table if not exists formatted_data
( user_id string,
  song_id string,
  artist_id string,
  timestamp string,
  start_ts string,
  end_ts string,
  geo_cd string,
  station_id string,
  song_end_type int,
  like int,
  dislike int
)partitioned by (batchid int)
row format delimited 
fields terminated by',';

