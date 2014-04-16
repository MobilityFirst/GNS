#
java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 66 -configFile ns.conf &
java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 134 -configFile ns.conf &
java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 2 -configFile ns.conf &
java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 78 -configFile ns.conf &
java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 4 -configFile ns.conf &
java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.main.StartLocalNameServer -id 1 -configFile lns.conf &
