#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
#echo $SCRIPTS
java -ea -DgigapaxosConfig=conf/gigapaxos.gnsApp.properties -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks -cp jars/GNS.jar edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNode -test -configFile $SCRIPTS/ns.properties &
java -ea -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks -cp jars/GNS.jar edu.umass.cs.gnsserver.localnameserver.LocalNameServer -configFile $SCRIPTS/lns.properties  &
