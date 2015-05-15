#
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.newApp.AppReconfigurableNode -test -nsfile ../../conf/name-server-info -debug &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.newApp.clientCommandProcessor.ClientCommandProcessor -port 20309 -nsfile ../../conf/name-server-info -debug &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -nsfile ../../conf/name-server-info -debug &
