#
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.newApp.AppReconfigurableNode -test -nsfile servers.txt -configFile ns_quiet.conf &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -nsfile servers.txt -configFile lns_quiet.conf &
