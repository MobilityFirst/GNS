#
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.newApp.AppReconfigurableNode -test -nsfile ../../conf/name-server-info &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.newApp.clientCommandProcessor.ClientCommandProcessor -host 10.0.1.50 -port 20309 -nsfile ../../conf/name-server-info &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer ../../conf/name-server-info &
