#!/bin/bash
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.gnsApp.AppReconfigurableNode -test -nsfile ../../conf/single-server-info -consoleOutputLevel WARNING -fileLoggingLevel WARNING -demandProfileClass edu.umass.cs.gns.gnsApp.NullDemandProfile  2>&1 | tee NSlogfile.txt &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -nsfile ../../conf/single-server-info -consoleOutputLevel WARNING -fileLoggingLevel WARNING 2>&1 | tee LNSlogfile.txt &
