use music;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=non-strict;
SET hive.auto.convert.join=false;

create table if not exists total_duration
( user_type string,
  duration int
)partitioned by (batchid int);

insert OVERWRITE table total_duration
partition (batchid=${hiveconf:batch})
select if((e.user_id = s.user_id) 
and (cast(e.timestamp as decimal(20,0)) <= cast(s.subscn_end_dt as decimal(20,0))),'subscribed','unsubscribed') as usertype, 
sum(abs(cast(e.end_ts as decimal (20,0))-cast(e.start_ts as decimal(20,0)))) as duration
from enriched_data e full outer join 
subscribed_users s on e.user_id = s.user_id where batchid = ${hiveconf:batch} and status = 'valid'
group by if((e.user_id = s.user_id) 
and (cast(e.timestamp as decimal(20,0)) <= cast(s.subscn_end_dt as decimal(20,0))),'subscribed','unsubscribed');

select * from total_duration;

       
