use music;
SET hive.auto.convert.join=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=non-strict;

create table if not exists top_10_connected_artist
(
  artist string,
  unique_user_count int
)partitioned by (batchid int);

INSERT OVERWRITE TABLE top_10_connected_artist
PARTITION(batchid=${hiveconf:batch})
SELECT 
ua.artistid, 
COUNT(DISTINCT ua.user_id) AS user_count
FROM
(
SELECT user_id, artistid FROM user_artist
LATERAL VIEW explode(artist_id) artists AS artistid
) ua
INNER JOIN
(
SELECT artist_id, song_id, user_id
FROM enriched_data
WHERE status='valid'
AND batchid=${hiveconf:batch}
) ed
ON ua.artistid=ed.artist_id
AND ua.user_id=ed.user_id
GROUP BY ua.artistid
ORDER BY user_count DESC
LIMIT 10;
 
select * from top_10_connected_artist where batchid = ${hiveconf:batch};
