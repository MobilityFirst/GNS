#
java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 0 -configFile ns.conf &
java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 1 -configFile ns.conf &
java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 2 -configFile ns.conf &
java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.main.StartLocalNameServer -id 3 -configFile lns.conf &


#./name-server.py --id 0 --nsFile name-server-info  --dataStore MONGO --location --consoleOutputLevel INFO --fileLoggingLevel FINE &
#./name-server.py --id 1 --nsFile name-server-info  --dataStore MONGO --location --consoleOutputLevel INFO --fileLoggingLevel FINE &
#./name-server.py --id 2 --nsFile name-server-info  --dataStore MONGO --location --consoleOutputLevel INFO --fileLoggingLevel FINE &
#./local-name-server.py --id 3 --nsFile name-server-info --runHttpServer --consoleOutputLevel INFO --fileLoggingLevel FINE &
