#
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.newApp.AppReconfigurableNode -test -nsfile ../../conf/single-server-info -consoleOutputLevel INFO -demandProfileClass edu.umass.cs.gns.newApp.NullDemandProfile -debugAPP -debugAR -debugPaxos -debugRecon -debugMisc > NSlog 2>&1 &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -nsfile ../../conf/single-server-info -consoleOutputLevel INFO > LNSlog 2>&1 &
