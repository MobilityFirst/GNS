#
java -ea -cp ../../dist/GNS.jar -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=../../conf/trustStore/node100.jks -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=../../conf/keyStore/node100.jks edu.umass.cs.gns.gnsApp.AppReconfigurableNode -test -nsfile ../../conf/single-server-info -configFile ns_debug.properties  &
# > NSlog 2>&1 &
java -ea -cp ../../dist/GNS.jar -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=../../conf/trustStore/node100.jks -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=../../conf/keyStore/node100.jks edu.umass.cs.gns.localnameserver.LocalNameServer -nsfile ../../conf/single-server-info -configFile lns.properties &
# > LNSlog 2>&1 &
# -Djavax.net.debug=ssl
