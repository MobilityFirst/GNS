#!/bin/bash
if [ -f LNSlogfile ]; then
mv --backup=numbered LNSlogfile LNSlogfile.save
fi
if [ -f NSlogfile ]; then
mv --backup=numbered NSlogfile NSlogfile.save
fi
nohup java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.newApp.AppReconfigurableNode -test -nsfile ../../conf/single-server-info -consoleOutputLevel WARNING -fileLoggingLevel WARNING -demandProfileClass edu.umass.cs.gns.newApp.NullDemandProfile > NSlogfile 2>&1 &
nohup java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -nsfile ../../conf/single-server-info -consoleOutputLevel WARNING -fileLoggingLevel WARNING > LNSlogfile 2>&1 &
