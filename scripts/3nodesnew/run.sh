#
java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id frank -nsfile servers.txt &
java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id smith -nsfile servers.txt &
java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id billy -nsfile servers.txt &
java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartLocalNameServer -address 10.0.1.50 -port 24398 -nsfile servers.txt &


