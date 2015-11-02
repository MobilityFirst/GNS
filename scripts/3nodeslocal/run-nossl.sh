#
java -Xms2048M -ea -DgigapaxosConfig=../../conf/gigapaxos.gnsApp.properties -cp ../../dist/GNS.jar  edu.umass.cs.gns.gnsApp.AppReconfigurableNode -test -configFile ns_nossl.properties  &
# > NSlog 2>&1 &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -configFile lns_nossl.properties &
# > LNSlog 2>&1 &

