#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
GNS=$SCRIPTS/../..
# to use IDE auto-build instead of ant
IDE_PATH=.:$GNS/build/classes:$GNS/build/test/classes:$GNS/lib/* 

java -Xms2048M -ea \
-cp $IDE_PATH:$GNS/jars/GNS.jar:$GNS/jars/GNSClient.jar \
-DgigapaxosConfig=$GNS/conf/gnsserver.1local.properties \
-Djava.util.logging.config.file=$GNS/conf/logging.gns.properties \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=$GNS/conf/trustStore/node100.jks \
-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=$GNS/conf/keyStore/node100.jks \
-DactiveConfig=$GNS/conf/activeCode/active.properties \
edu.umass.cs.reconfiguration.ReconfigurableNode \
-enableActiveCode -configFile \
$SCRIPTS/ns.properties \
START_ALL &
# START_ALL starts all nodes for a single node test; else should
# explicitly specify nodes as trailing command-line args

# comment to start optional LNS
#exit

java -ea  -cp $IDE_PATH:jars/GNS.jar \
-DgigapaxosConfig=conf/gnsserver.1local.properties \
-Djava.util.logging.config.file=conf/logging.gns.properties \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/trustStore/node100.jks \
-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
edu.umass.cs.gnsserver.localnameserver.LocalNameServer -configFile \
$SCRIPTS/lns.properties  &
