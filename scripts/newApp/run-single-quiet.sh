#!/bin/bash
#if [ -f LNSlogfile ]; then
#mv --backup=numbered LNSlogfile LNSlogfile.save
#fi
#if [ -f NSlogfile ]; then
#mv --backup=numbered NSlogfile NSlogfile.save
#fi
#
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.newApp.AppReconfigurableNode -test -nsfile ../../conf/single-server-info -consoleOutputLevel WARNING -fileLoggingLevel WARNING -demandProfileClass edu.umass.cs.gns.newApp.NullDemandProfile  2>&1 | tee NSlogfile &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -nsfile ../../conf/single-server-info -consoleOutputLevel WARNING -fileLoggingLevel WARNING 2>&1 | tee LNSlogfile &
