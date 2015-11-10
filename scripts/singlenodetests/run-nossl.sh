#
java -ea -DgigapaxosConfig=../../conf/gigapaxos.gnsApp.properties -cp ../../dist/GNS.jar edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNode -test -configFile ns_nossl.properties  &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gnsserver.localnameserver.LocalNameServer -configFile lns_nossl.properties &

