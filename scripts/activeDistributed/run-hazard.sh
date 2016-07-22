#!/bin/bash
# THIS FILE IS DIFFERENT FROM THE STANDARD SINGLE NODE ONLY IN USING NON-STANDARD PORT NUMBERS
# SEE gnsserver.1hazard.properties for details.
#
SCRIPTS="`dirname \"$0\"`"
#echo $SCRIPTS
IDE_PATH=build/classes:build/test/classes:lib/*:

if [[ "$OSTYPE" != "darwin"* ]]; then
if [ -f LNSlogfile ]; then
mv --backup=numbered LNSlogfile LNSlogfile.save
fi
if [ -f NSlogfile ]; then
mv --backup=numbered NSlogfile NSlogfile.save
fi
fi



nohup java -Xms2048M -ea -cp $IDE_PATH:jars/GNS.jar \
-Djava.util.logging.config.file=conf/logging.gns.properties \
-DgigapaxosConfig=conf/gnsserver.1hazard.properties \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/trustStore/node100.jks \
-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
edu.umass.cs.reconfiguration.ReconfigurableNode \
-test -disableEmailVerification -configFile $SCRIPTS/ns_hazard.properties \
START_ALL > NSlogfile 2>&1 &
# START_ALL starts all nodes for a single node test; else should
# explicitly specify nodes as trailing command-line args

# comment to start optional LNS
exit

nohup java -ea -cp $IDE_PATH:jars/GNS.jar \
-Djava.util.logging.config.file=conf/logging.gns.properties \
-DgigapaxosConfig=conf/gnsserver.1hazard.properties \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/trustStore/node100.jks \
-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
edu.umass.cs.gnsserver.localnameserver.LocalNameServer -configFile \
$SCRIPTS/lns_hazard.properties > LNSlogfile 2>&1 &
