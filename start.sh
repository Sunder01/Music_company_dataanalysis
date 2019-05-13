#!/bin/bash
pass=$0
start-all.sh
echo $pass | sudo -S service mysqld start 
cd /usr/local/hadoop-2.6.0/sbin
./mr-jobhistory-daemon.sh start historyserver
cd ~
start-hbase.sh



