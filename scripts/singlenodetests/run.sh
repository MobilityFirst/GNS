#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
IDE_PATH=build/classes:build/test/classes:lib/*:

java -Xms2048M -ea -cp $IDE_PATH:jars/GNS.jar \
-Djava.util.logging.config.file=conf/logging.gns.properties \
-DgigapaxosConfig=conf/gigapaxos.server.singleNode.local.properties \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/trustStore/node100.jks \
-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
edu.umass.cs.reconfiguration.ReconfigurableNode \
-test -disableEmailVerification -configFile $SCRIPTS/ns.properties \
START_ALL &
# START_ALL starts all nodes for a single node test; else should
# explicitly specify nodes as trailing command-line args

# comment to start optional LNS
#exit

java -ea -DgigapaxosConfig=conf/gigapaxos.server.singleNode.local.properties \
-Djava.util.logging.config.file=conf/logging.gns.properties \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/trustStore/node100.jks \
-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks -cp $IDE_PATH:jars/GNS.jar \
edu.umass.cs.gnsserver.localnameserver.LocalNameServer -configFile \
$SCRIPTS/lns.properties  &
