#

java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id frank -configFile ns.conf &
java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id sammy -configFile ns.conf &
java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id billy -configFile ns.conf &
java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartLocalNameServer -address 10.0.1.50 -port 24398 -configFile lns.conf &
