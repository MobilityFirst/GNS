#!/bin/bash
if [ -f LNSlogfile ]; then
mv --backup=numbered LNSlogfile LNSlogfile.save
fi
if [ -f NSlogfile ]; then
mv --backup=numbered NSlogfile NSlogfile.save
fi
#java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.newApp.AppReconfigurableNode -test -configFile ns_hazard.conf &
#java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -configFile lns_hazard.conf &
nohup java -ea -DgigapaxosConfig=../../conf/gigapaxos.gnsApp.properties -cp ../../dist/GNS.jar edu.umass.cs.gns.gnsApp.AppReconfigurableNode -test -configFile ns_hazard.properties > NSlogfile 2>&1 &
nohup java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -configFile lns_hazard.properties > LNSlogfile 2>&1 &
