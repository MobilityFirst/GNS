#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
# to use IDE auto-build instead of ant
IDE_PATH=.:build/classes:build/test/classes:lib/* 

# deprecated bad way to start
#java -Xms2048M -ea -cp $IDE_PATH:jars/GNS.jar -Dlog4j.configuration=log4j.properties -DgigapaxosConfig=conf/gigapaxos.gnsapp.properties -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks edu.umass.cs.gnsserver.gnsapp.AppReconfigurableNode  -test -configFile $SCRIPTS/ns.properties &

# new good way to start
java -Xms2048M -ea -cp $IDE_PATH:jars/GNS.jar \
-DgigapaxosConfig=conf/gigapaxos.gnsApp.properties \
-Djava.util.logging.config.file=logging.properties \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/trustStore/node100.jks \
-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
edu.umass.cs.reconfiguration.ReconfigurableNode \
-test -disableEmailVerification -configFile \
$SCRIPTS/ns.properties \
START_ALL &
# START_ALL starts all nodes for a single node test; else should
# explicitly specify nodes as trailing command-line args

# comment to also start a local name server
#exit

# LNS is optional. An LNS in general is not associated with any
# specific active replica, so there isn't a good reason to start the
# two together except perhaps for very specific test settings. LNSes
# are more like clients and hardly like servers.
java -ea -cp $IDE_PATH:jars/GNS.jar \
-DgigapaxosConfig=conf/gigapaxos.gnsApp.properties \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/trustStore/node100.jks \
-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
edu.umass.cs.gnsserver.localnameserver.LocalNameServer -configFile \
$SCRIPTS/lns.properties &
