#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
#echo $SCRIPTS
IDE_PATH=build/classes:build/test/classes:lib/* 
java -Xms2048M -ea -cp $IDE_PATH:jars/GNS.jar -DgigapaxosConfig=conf/gigapaxos.gnsApp.properties -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNode -test -debugNio -configFile $SCRIPTS/ns.properties 2>/tmp/log &
java -ea -cp $IDE_PATH:jars/GNS.jar -DgigapaxosConfig=conf/gigapaxos.gnsApp.properties -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks edu.umass.cs.gnsserver.localnameserver.LocalNameServer -configFile $SCRIPTS/lns.properties &
