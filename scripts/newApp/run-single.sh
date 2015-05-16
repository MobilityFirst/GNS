#
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.newApp.AppReconfigurableNode -test -nsfile ../../conf/single-server-info -debug &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.newApp.clientCommandProcessor.ClientCommandProcessor -port 20309 -nsfile ../../conf/single-server-info -activeReplicaID frank -debug &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -nsfile ../../conf/single-server-info -debug &
