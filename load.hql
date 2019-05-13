use music;

SET hive.auto.convert.join=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=non-strict;

truncate table formatted_data;
truncate table enriched_data;
truncate table user_artist;

load data inpath '/user/acadgild/data/batch_${hiveconf:batch}/mob/file.txt' into table formatted_data partition (batchid = ${hiveconf:batch});
load data inpath '/user/acadgild/data/batch_${hiveconf:batch}/web/xml_csv${hiveconf:batch}/part-m-00000' into table formatted_data partition (batchid = ${hiveconf:batch});
load data local inpath '/home/acadgild/music/data/user-artist.txt' into table user_artist;

insert into table enriched_data
partition(batchid,status)
SELECT 
i.user_id,
i.song_id,
sa.artist_id,
i.timestamp,
i.start_ts,
i.end_ts,
sg.geo_cd,
i.station_id,
IF (i.song_end_type IS NULL, 3, i.song_end_type) AS song_end_type,
IF (i.like IS NULL, 0, i.like) AS like,
IF (i.dislike IS NULL, 0, i.dislike) AS dislike,
i.batchid,
IF((i.like=1 AND i.dislike=1) 
OR i.user_id IS NULL 
OR i.song_id IS NULL
OR i.timestamp IS NULL
OR i.start_ts IS NULL
OR i.end_ts IS NULL
OR i.geo_cd IS NULL
OR i.user_id='' 
OR i.song_id='' 
OR i.timestamp='' 
OR i.start_ts='' 
OR i.end_ts='' 
OR i.geo_cd=''
OR sg.geo_cd IS NULL
OR sg.geo_cd=''
OR sa.artist_id IS NULL
OR sa.artist_id='', 'invalid', 'valid') AS status
FROM formatted_data i 
LEFT OUTER JOIN station_geo_map sg ON i.station_id = sg.station_id
LEFT OUTER JOIN song_artist_map sa ON i.song_id = sa.song_id
WHERE i.batchid=${hiveconf:batch};


