#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
IDE_PATH=build/classes:build/test/classes:lib/*:

# deprecated bad way
#java -ea -DgigapaxosConfig=conf/gigapaxos.gnsApp.properties -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks -cp jars/GNS.jar edu.umass.cs.gnsserver.gnsapp.AppReconfigurableNode -test -configFile $SCRIPTS/ns.properties &
# new good way to start
java -Xms2048M -ea -cp $IDE_PATH:jars/GNS.jar \
-Dlog4j.configuration=log4j.properties \
-DgigapaxosConfig=conf/gigapaxos.gnsapp.singleNode.properties \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/trustStore/node100.jks \
-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
edu.umass.cs.reconfiguration.ReconfigurableNode -test -enableActiveCode -configFile \
$SCRIPTS/ns.properties \
START_ALL &
# START_ALL starts all nodes for a single node test; else should
# explicitly specify nodes as trailing command-line args


java -ea -DgigapaxosConfig=conf/gigapaxos.gnsApp.singleNode.properties -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks -cp jars/GNS.jar edu.umass.cs.gnsserver.localnameserver.LocalNameServer -configFile $SCRIPTS/lns.properties  &
