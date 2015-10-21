#
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.gnsApp.AppReconfigurableNode -test -nsfile ../../conf/single-server-info -consoleOutputLevel INFO -debugAPP -demandProfileClass edu.umass.cs.gns.gnsApp.NullDemandProfile &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -nsfile ../../conf/single-server-info -consoleOutputLevel INFO -debug &
