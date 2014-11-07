#
java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 0 -configFile ns.conf &
java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 1 -configFile ns.conf &
java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 2 -configFile ns.conf &
java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartLocalNameServer -address 10.0.1.50 -port 24398 -configFile lns.conf &
# -gigapaxos
#java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id henry -configFile ns.conf &
#java -Xmx2g -cp ../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id crash -configFile ns.conf &
#s

