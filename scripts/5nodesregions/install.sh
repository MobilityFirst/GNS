#!/bin/bash

sudo yum --assumeyes update
sudo yum --assumeyes install emacs ant ant-junit svn mysql mysql-server

# more ant stuff that might be needed
sudo yum --assumeyes install ant-apache*

sudo /etc/init.d/mysqld start

sudo /usr/bin/mysql_install_db 

sudo /usr/bin/mysqladmin -u root password 'toorbar'

sudo mysqladmin -u root --password=toorbar -v create gnrs

# make it point to the JDK not the JRE
export JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/

svn checkout svn+ssh://westy@none.cs.umass.edu/nfs/none/users3/hardeep/svn-mobility-first/GNRS-westy

# svn checkout svn+ssh://westy@none.cs.umass.edu/nfs/none/users3/westy/svn-mobility-first/GCRS

pushd GNRS-westy

# get rid of test code
#rm -r test

ant

#java -classpath  "build/classes:lib/*"  edu.umass.cs.gcrs.gcrs.GCRS