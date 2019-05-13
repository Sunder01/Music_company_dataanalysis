#!/bin/bash

chmod 775 /home/acadgild/start.sh

if [ -f "/home/acadgild/music/batch/batch.txt" ]
then
echo "Batch found!"
else
echo -n "1">"/home/acadgild/music/batch/batch.txt"
fi
if [ -f "/home/acadgild/music/batch/hbase.txt" ]
then
echo "Hbase recorder created!"
else
echo -n "1">"/home/acadgild/music/batch/hbase.txt"
fi
chmod 775 /home/acadgild/music/batch/batch.txt
batch=`cat /home/acadgild/music/batch/batch.txt`
hbase=`cat /home/acadgild/music/batch/hbase.txt`

hadoop fs -rmr /user/acadgild/data/batch_$batch
hadoop fs -mkdir -p /user/acadgild/data/batch_$batch/mob
hadoop fs -mkdir -p /user/acadgild/data/batch_$batch/web
rm -r /home/acadgild/batch_$batch

mkdir -p /home/acadgild/batch_$batch/mob/
mkdir -p /home/acadgild/batch_$batch/web/

cp /home/acadgild/music/file.txt /home/acadgild/batch_$batch/mob/
cp /home/acadgild/music/file.xml /home/acadgild/batch_$batch/web/

echo "putting mobile data to HDFS" >> /home/acadgild/music/logfile_$batch
hadoop fs -put /home/acadgild/batch_$batch/mob/file.txt /user/acadgild/data/batch_$batch/mob/
echo "putting web data to HDFS" >> logfile
hadoop fs -put /home/acadgild/batch_$batch/web/file.xml /user/acadgild/data/batch_$batch/web/

echo "executing pig script" >> /home/acadgild/music/logfile_$batch
pig -param batch=$batch /home/acadgild/pig_csv.pig
echo "creating hbase tables" >> /home/acadgild/music/logfile_$batch
if [ $hbase -eq 1 ]
then
./hbaseddl.sh
echo "hbase created"
else
echo "hbase created"
fi

hive -f /home/acadgild/hive_hbase.hql 
echo "Creating hive tables" >> /home/acadgild/music/logfile_$batch
hive -f /home/acadgild/musicddl.hql
echo "Loading data" >> /home/acadgild/music/logfile_$batch
hive -hiveconf batch=$batch -f /home/acadgild/load.hql
echo "performing analysis" >> /home/acadgild/music/logfile_$batch
hive -hiveconf batch=$batch -f /home/acadgild/analysis1.hql
hive -hiveconf batch=$batch -f /home/acadgild/analysis2.hql
hive -hiveconf batch=$batch -f /home/acadgild/analysis3.hql
hive -hiveconf batch=$batch -f /home/acadgild/analysis4.hql
hive -hiveconf batch=$batch -f /home/acadgild/analysis5.hql

echo -n $batch+1 >> /home/acadgild/music/batch.txt
echo -n $hbase+1 >> /home/acadgild/music/batch.txt

