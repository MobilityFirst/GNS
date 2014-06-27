#!/bin/bash
yum --quiet --assumeyes update
yum --quiet --assumeyes install java-1.7.0-openjdk 
echo "[MongoDB]
name=MongoDB Repository
baseurl=http://downloads-distro.mongodb.org/repo/redhat/os/x86_64
gpgcheck=0
enabled=1" > mongodb.repo
mv mongodb.repo /etc/yum.repos.d/mongodb.repo
yum --quiet --assumeyes install mongodb-org
service mongod start