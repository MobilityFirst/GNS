#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
#echo $SCRIPTS
java -Xms2048M -ea -DgigapaxosConfig=conf/gigapaxos.gnsApp.properties -cp jars/GNS.jar -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNode -test -configFile $SCRIPTS/ns.properties &
java -ea -cp jars/GNS.jar -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks edu.umass.cs.gnsserver.localnameserver.LocalNameServer -configFile $SCRIPTS/lns.properties &
