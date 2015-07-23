#
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.newApp.AppReconfigurableNode -test -nsfile ../../conf/single-server-info -configFile ns_debug.conf &
# > NSlog 2>&1 &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -nsfile ../../conf/single-server-info -configFile lns.conf &
# > LNSlog 2>&1 &
