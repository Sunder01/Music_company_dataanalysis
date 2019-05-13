use music;
create external table if not exists station_geo_map
     (
     station_id String,
     geo_cd string
     )
     STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
     with serdeproperties
     ("hbase.columns.mapping"=":key,geo_cd:id") 
     tblproperties("hbase.table.name"="Station_Geo_Map");

create external table if not exists subscribed_users
     (
     user_id STRING,
     subscn_start_dt STRING,
     subscn_end_dt STRING
     )
     STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
     with serdeproperties
     ("hbase.columns.mapping"=":key,subscn:start,subscn:end")    
     tblproperties("hbase.table.name"="Subscribed_User");

create external table if not exists song_artist_map
     (
     song_id STRING,
     artist_id STRING
     )
     STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
     with serdeproperties
     ("hbase.columns.mapping"=":key,Artist_id:id")   
     tblproperties("hbase.table.name"="Song_Artist_Map");

