#!/bin/bash

su --session-command="/usr/bin/yum --assumeyes update"
su --session-command="/usr/bin/yum --assumeyes install emacs ant ant-junit svn mysql mysql-server"

# more ant stuff that might be needed
su --session-command="/usr/bin/yum --assumeyes --assumeyes install ant-apache*"

su --session-command="/etc/init.d/mysqld start"

su --session-command="/usr/bin/mysql_install_db"

su --session-command="/usr/bin/mysqladmin -u root password 'toorbar'"

su --session-command="/usr/bin/mysqladmin -u root --password=toorbar -v create gnrs"

# make it point to the JDK not the JRE
export JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/

svn checkout svn+ssh://westy@none.cs.umass.edu/nfs/none/users3/hardeep/svn-mobility-first/GNRS-westy

# svn checkout svn+ssh://westy@none.cs.umass.edu/nfs/none/users3/westy/svn-mobility-first/GCRS

pushd GNRS-westy

# get rid of test code
#rm -r test

ant

#java -classpath  "build/classes:lib/*"  edu.umass.cs.gcrs.gcrs.GCRS