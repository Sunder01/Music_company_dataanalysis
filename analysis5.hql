use music;
SET hive.auto.convert.join=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=non-strict;


create table if not exists top_10_unsubscribed_user
(
  user_id string,
  duration int
)partitioned by (batchid int);

insert OVERWRITE table top_10_unsubscribed_user
partition(batchid=${hiveconf:batch})
select e.user_id,sum(abs(cast(e.end_ts as decimal(20,0))-cast(e.start_ts as decimal(20,0)))) as duration
       from enriched_data e left outer join subscribed_users s on e.user_id = s.user_id where e.user_id != s.user_id 
       or s.subscn_end_dt > e.timestamp and batchid = ${hiveconf:batch} group by e.user_id 
       order by duration desc;

select * from top_10_unsubscribed_user where batchid = ${hiveconf:batch};

