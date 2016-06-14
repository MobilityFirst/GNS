#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
# to use IDE auto-build instead of ant
IDE_PATH=.:build/classes:build/test/classes:lib/* 

java -ea -cp $IDE_PATH:jars/GNS.jar \
-DgigapaxosConfig=conf/gigapaxos.server.gnserve.net.3node.properties \
-Djava.util.logging.config.file=conf/logging.gns.properties \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/trustStore/node100.jks \
-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
edu.umass.cs.reconfiguration.ReconfigurableNode \
-disableEmailVerification -configFile \
$SCRIPTS/ns.properties \
useast1_recon useast1_repl &
# START_ALL starts all nodes for a single node test; else should
# explicitly specify nodes as trailing command-line args

# comment to start optional LNS
exit

java -ea  -cp $IDE_PATH:jars/GNS.jar \
-DgigapaxosConfig=conf/gigapaxos.server.gnserve.net.3node.properties \
-Djava.util.logging.config.file=conf/logging.gns.properties \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/trustStore/node100.jks \
-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
edu.umass.cs.gnsserver.localnameserver.LocalNameServer -configFile \
$SCRIPTS/lns.properties  &
