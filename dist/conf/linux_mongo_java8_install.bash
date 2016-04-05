#!/bin/bash
# make current directory the directory this script is in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR
#
wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u5-b13/jre-8u5-linux-x64.tar.gz  
tar -zxvf jre-8u5-linux-x64.tar.gz
# link java to java bin
ln -sf jre1.8.0_05/bin/java
# now do mongo
wget http://downloads.mongodb.org/linux/mongodb-linux-x86_64-2.6.3.tgz
tar -zxvf mongodb-linux-x86_64-2.6.3.tgz
# create the data dir
mkdir -p mongodata
nohup mongodb-linux-x86_64-2.6.3/bin/mongod --dbpath mongodata --smallfiles > MONGOlogfile 2>&1 &
