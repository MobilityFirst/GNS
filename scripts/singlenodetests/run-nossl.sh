#
java -ea -DgigapaxosConfig=../../conf/gigapaxos.gnsApp.properties -cp ../../dist/GNS.jar edu.umass.cs.gns.gnsApp.AppReconfigurableNode -test -configFile ns_nossl.properties  &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -configFile lns_nossl.properties &

