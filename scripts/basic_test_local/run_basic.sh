# run mongod
killall -9 mongod
rm -rf gnsdb
mkdir -p gnsdb
nohup mongod --dbpath=gnsdb  &

# run name servers and local name servers
killall -9 java
rm -rf log
mkdir -p log/ns0 log/ns1 log/ns2 log/lns3

cd log/ns0
nohup java -ea -cp ../../../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 0 -nsfile ../../node_config &
cd ../../

cd log/ns1
nohup java -ea -cp ../../../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 1 -nsfile ../../node_config &
cd ../../

cd log/ns2
nohup java -ea -cp ../../../../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 2 -nsfile ../../node_config &
cd ../../

cd log/lns3
nohup java -ea -cp ../../../../dist/GNS.jar edu.umass.cs.gns.main.StartLocalNameServer -id 3 -nsfile ../../node_config -experimentMode  -statFileLoggingLevel FINE -statConsoleOutputLevel FINE &
cd ../../
