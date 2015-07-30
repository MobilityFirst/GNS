#!/bin/bash
# make current directory the directory this script is in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR
#
yum --quiet --assumeyes update
# sometimes we need emacs
yum --quiet --assumeyes install emacs
yum --quiet --assumeyes install java-1.7.0-openjdk 
# install and start mongodb
echo "[MongoDB]
name=MongoDB Repository
baseurl=http://downloads-distro.mongodb.org/repo/redhat/os/x86_64
gpgcheck=0
enabled=1" > mongodb.repo
mv mongodb.repo /etc/yum.repos.d/mongodb.repo
yum --quiet --assumeyes install mongodb-org
service mongod start
# fix the sudoers so ssh sudo works all the time
chmod ug+rw /etc/sudoers
sed -i 's/requiretty/!requiretty/' /etc/sudoers
# install and start mysql
yum --quiet --assumeyes install mysql-server
service mysqld start
mysqladmin -u root password 'gnsRoot'