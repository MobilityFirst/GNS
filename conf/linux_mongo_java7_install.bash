#!/bin/bash
wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/7u60-b19/jre-7u60-linux-x64.tar.gz
tar -zxvf jre-7u60-linux-x64.tar.gz
# link java to java bin
ln -s jre1.7.0_60/bin/java
# now do mongo
wget http://downloads.mongodb.org/linux/mongodb-linux-x86_64-2.6.3.tgz
tar -zxvf mongodb-linux-x86_64-2.6.3.tgz
# create the data dir
mkdir -p mongodata
nohup mongodb-linux-x86_64-2.6.3/bin/mongod --dbpath mongodata --smallfiles > MONGOlogfile 2>&1 &
