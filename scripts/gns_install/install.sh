#!/bin/bash

#All the installations will happen in home directory

cd $HOME

status=0

wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/7u55-b13/jdk-7u55-linux-i586.tar.gz

if [ $? -eq 0 ]
then
  echo "jdk is sucessfully downloaded"
  tar -xvf jdk*
else
  echo "jdk unsuccessfully downloaded"
  status=1
fi


wget http://downloads.mongodb.org/linux/mongodb-linux-i686-2.6.1.tgz

if [ $? -eq 0 ]
then
  echo "mongodb is sucessfully downloaded"
  tar -xvf mongodb*
else
  echo "mongodb unsuccessfully downloaded"
  status=1
fi

if [ status -eq 0 ]
then
 export PATH=$PATH:$HOME/jdk1.7.0_55/bin/:$HOME/mongodb-linux-i686-2.6.1/bin
 exit 0
else
 echo "Error in Script Execution"
 exit 1 
fi

#-----------End of Installation Script ----------------------------------------
