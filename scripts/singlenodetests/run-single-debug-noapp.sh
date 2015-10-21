#
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.gnsApp.AppReconfigurableNode -test -nsfile ../../conf/single-server-info -consoleOutputLevel INFO  -demandProfileClass edu.umass.cs.gns.gnsApp.NullDemandProfile -debugAR -debugPaxos -debugRecon &
#java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.gnsApp.clientCommandProcessor.ClientCommandProcessor -port 20309 -nsfile ../../conf/single-server-info -activeReplicaID frank -consoleOutputLevel INFO -debug &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -nsfile ../../conf/single-server-info -consoleOutputLevel WARNING &
