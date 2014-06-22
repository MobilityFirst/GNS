gns_jar=$1
client_port=34242

if [ -z "$gns_jar" ]; then
        echo "ERROR: No GNS jar given. Give complete path to GNS jar."
        echo "Usage: ./run_basic.sh <Path to GNS jar>"
        exit 2
fi


# run mongod
killall -9 mongod
rm -rf gnsdb
mkdir -p gnsdb
nohup mongod --dbpath=gnsdb  &


# run name servers and local name servers
killall -9 java
rm -rf log
mkdir -p log/ns0 log/ns1 log/ns2 log/lns1 log/client

cd log/ns0
nohup java -ea -cp $gns_jar edu.umass.cs.gns.main.StartNameServer -id 0 -nsfile ../../node_config_1node -singleNS &
cd ../../


# start local name server
cd log/lns1
nohup java -ea -cp $gns_jar edu.umass.cs.gns.main.StartLocalNameServer -id 1 -nsfile ../../node_config_1node -primary 1  \
 -experimentMode -statFileLoggingLevel FINE -statConsoleOutputLevel FINE &
cd ../../

# sleep so that LNS can startup and bind itself to rmiregistry
sleep 2

cd log/client
java -ea -cp $gns_jar edu.umass.cs.gns.test.nioclient.ClientSample ../../node_config_1node $client_port
cd ../../
