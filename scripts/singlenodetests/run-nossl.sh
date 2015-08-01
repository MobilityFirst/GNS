#
java -ea -cp ../../dist/GNS.jar  edu.umass.cs.gns.newApp.AppReconfigurableNode -test -configFile ns_nossl.conf  &
# > NSlog 2>&1 &
java -ea -cp ../../dist/GNS.jar edu.umass.cs.gns.localnameserver.LocalNameServer -configFile lns_nossl.conf &
# > LNSlog 2>&1 &

