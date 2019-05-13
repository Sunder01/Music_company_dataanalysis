use music;
SET hive.exec.dynamic.partition=TRUE;
SET hive.exec.dynamic.partition.mode=non-strict;

create table if not exists top_10_royalty
(
 song_id string,
 duration int
)partitioned by (batchid int);



insert OVERWRITE table top_10_royalty
partition(batchid=${hiveconf:batch})
select song_id,sum(abs(cast(end_ts as DECIMAL(20,0))-cast(start_ts as DECIMAL(20,0)))) as duration from enriched_data 
where batchid = ${hiveconf:batch} and (like = 1 or song_end_type=0) and status = 'valid' group by song_id order by duration desc ;


select * from top_10_royalty where batchid = ${hiveconf:batch};
