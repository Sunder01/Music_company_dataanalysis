start-hbase.sh
echo "disable 'Station_Geo_Map'" | hbase shell
echo "is_disabled 'Station_Geo_Map'" | hbase shell
echo "drop 'Station_Geo_Map'" | hbase shell
echo "disable 'Song_Artist_Map'" | hbase shell
echo "is_disabled 'Song_Artist_Map'" | hbase shell
echo "drop 'Song_Artist_Map'" | hbase shell
echo "disable 'Subscribed_User'" | hbase shell
echo "is_disabled 'Subscribed_User'" | hbase shell
echo "drop 'Subscribed_User'" | hbase shell

echo "create 'Station_Geo_Map','Station_id','geo_cd'" | hbase shell
echo "create 'Song_Artist_Map','Song_id','Artist_id'" | hbase shell
echo "create 'Subscribed_User','user_id','subscn'" | hbase shell


file="/home/acadgild/music/song-artist.txt"
while IFS= read -r line
do
 songid=`echo $line | cut -d',' -f1`
 artist=`echo $line | cut -d',' -f2`
 echo "put 'Song_Artist_Map', '$songid', 'Artist_id:id', '$artist'" | hbase shell
done <"$file"

file="/home/acadgild/music/stn-geocd.txt"
while IFS= read -r line
do 
   station=`echo $line | cut -d ',' -f1`
   geocd= `eccho $line | cut -d ',' -f2`
   echo "put 'Station_Geo_Map','$station','geo_cd:id','$geocd'" | hbase shell
done <"$file"

file="/home/acadgild/music/user-subscn.txt"
while IFS= read -r line
do 
 user=`echo $line | cut -d',' -f1`
 start=`echo $line | cut -d',' -f2`
 end=`echo $line | cut -d',' -f3`
 echo "put 'Subscribed_User','$user','subscn:start','$start'" | hbase shell
 echo "put 'Subscribed_User','$user','subscn:end','$end'" | hbase shell
done <"$file"

