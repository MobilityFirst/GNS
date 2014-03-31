#

java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.nsdesign.StartNameServer -id 0 -configFile ns.conf &
java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.nsdesign.StartNameServer -id 1 -configFile ns.conf &
java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.nsdesign.StartNameServer -id 2 -configFile ns.conf &
java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.main.StartLocalNameServer -id 3 -configFile lns.conf &
