#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
#echo $SCRIPTS
java -Xms2048M -ea -DgigapaxosConfig=conf/gigapaxos.gnsApp.properties -cp jars/GNS.jar  edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNode -test -configFile $SCRIPTS/ns_nossl.properties  &
# > NSlog 2>&1 &
java -ea -cp jars/GNS.jar edu.umass.cs.gnsserver.localnameserver.LocalNameServer -configFile $SCRIPTS/lns_nossl.properties &
# > LNSlog 2>&1 &

