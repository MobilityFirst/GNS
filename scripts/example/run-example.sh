#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
java -ea -DgigapaxosConfig=conf/gigapaxos.gnsApp.example.properties -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks -cp jars/GNS.jar edu.umass.cs.gnsserver.gnsapp.AppReconfigurableNode -test -configFile $SCRIPTS/ns_example.properties &
java -ea -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks -cp jars/GNS.jar edu.umass.cs.gnsserver.localnameserver.LocalNameServer -configFile $SCRIPTS/lns_example.properties &
