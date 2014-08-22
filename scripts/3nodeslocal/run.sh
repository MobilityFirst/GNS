#

java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 0 -configFile ns.conf &
java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 1 -configFile ns.conf &
java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 2 -configFile ns.conf &
java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartLocalNameServer -id 3 -address 10.0.1.50 -port 24398 -configFile lns.conf &
