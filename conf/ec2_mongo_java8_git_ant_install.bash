#!/bin/bash
# make current directory the directory this script is in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR
#
yum --quiet --assumeyes update
yum --quiet --assumeyes install java-1.8.0-openjdk-devel
yum --quiet --assumeyes remove java-1.7.0-openjdk
yum --quiet --assumeyes install git
yum --quiet --assumeyes install emacs
#
# MONGO
echo "[MongoDB]
name=MongoDB Repository
baseurl=http://downloads-distro.mongodb.org/repo/redhat/os/x86_64
gpgcheck=0
enabled=1" > mongodb.repo
mv mongodb.repo /etc/yum.repos.d/mongodb.repo
yum --quiet --assumeyes install mongodb-org
service mongod start
#
# fix the sudoers so ssh sudo works all the time
chmod ug+rw /etc/sudoers
sed -i 's/requiretty/!requiretty/' /etc/sudoers
#
# install and start mysql
yum --quiet --assumeyes install mysql-server
service mysqld start
mysqladmin -u root password 'gnsRoot'
#
# ANT
#remove old
yum --quiet --assumeyes remove ant
cd /opt/
#download ant                                                                                                                  
wget http://mirrors.ibiblio.org/apache//ant/binaries/apache-ant-1.9.7-bin.zip
#unzip and delete zip(your choice)
unzip apache-ant-1.9.7-bin.zip && rm apache-ant-1.9.7-bin.zip
cd apache-ant-1.9.7/
#create symlinks                                                                                      
ln -f -s /opt/apache-ant-1.9.7 /opt/ant
ln -f -s /opt/ant/bin/ant /usr/bin/ant
