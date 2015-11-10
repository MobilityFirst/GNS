#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
#echo $SCRIPTS
if [ -f LNSlogfile ]; then
mv --backup=numbered LNSlogfile LNSlogfile.save
fi
if [ -f NSlogfile ]; then
mv --backup=numbered NSlogfile NSlogfile.save
fi
nohup java -ea -DgigapaxosConfig=conf/gigapaxos.gnsApp.properties -cp jars/GNS.jar edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNode -test -configFile $SCRIPTS/ns_hazard.properties > NSlogfile 2>&1 &
nohup java -ea -cp jars/GNS.jar edu.umass.cs.gnsserver.localnameserver.LocalNameServer -configFile $SCRIPTS/lns_hazard.properties > LNSlogfile 2>&1 &
