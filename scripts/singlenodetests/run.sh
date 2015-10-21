#
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.gnsApp.AppReconfigurableNode -test -nsfile ../../conf/name-server-info -consoleOutputLevel INFO  -demandProfileClass edu.umass.cs.gns.gnsApp.NullDemandProfile -debugAPP -debugAR -debugPaxos -debugRecon &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -nsfile ../../conf/name-server-info -debug &
