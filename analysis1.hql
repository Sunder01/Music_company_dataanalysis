use music;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=non-strict;

truncate table top_10_station;
create table if not exists top_10_station
(
 station_id string,
 song_count string,
 distinct_user string
)partitioned by(batchid int);

INSERT OVERWRITE table top_10_station
partition(batchid=${hiveconf:batch})
select station_id,count(song_id) as song,count(distinct user_id) 
       from enriched_data where like = 1 and batchid = ${hiveconf:batch}
       and status = 'valid' 
       group by station_id order by song desc limit 10;

select * from top_10_station where batchid = ${hiveconf:batch};
