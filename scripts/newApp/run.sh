#
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.newApp.AppReconfigurableNode -test -nsfile ../../conf/name-server-info -consoleOutputLevel INFO  -demandProfileClass edu.umass.cs.gns.newApp.NullDemandProfile -debugAPP -debugAR -debugPaxos -debugRecon &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -nsfile ../../conf/name-server-info -debug &
