#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
#echo $SCRIPTS
java -ea -DgigapaxosConfig=conf/gigapaxos.gnsApp.properties -cp jars/GNS.jar edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNode -test -configFile $SCRIPTS/ns_nossl.properties  &
java -ea -cp jars/GNS.jar edu.umass.cs.gnsserver.localnameserver.LocalNameServer -configFile $SCRIPTS/lns_nossl.properties &

