gns_jar=$1
client_port=34242

if [ -z "$gns_jar" ]; then
        echo "ERROR: No GNS jar given. Give complete path to GNS jar."
        echo "Usage: ./run_basic.sh <Path to GNS jar>"
        exit 2
fi

##
# NOTE: The '-multipaxos' option at name server enables the use of multipaxos package. 
# To use the earlier paxos implementation, remove the '-multipaxos' option from name server commands.
##

# run mongod
killall -9 mongod
rm -rf gnsdb
mkdir -p gnsdb
nohup mongod --dbpath=gnsdb  > mongo.out 2> mongo.out &


# run name servers and local name servers
killall -9 java
rm -rf log
mkdir -p log/ns0 log/ns1 log/ns2 log/lns3 log/client

cd log/ns0
nohup java -ea -cp $gns_jar edu.umass.cs.gns.main.StartNameServer -multipaxos -id 0 -nsfile ../../node_config &
cd ../../

cd log/ns1
nohup java -ea -cp $gns_jar edu.umass.cs.gns.main.StartNameServer -multipaxos -id 1 -nsfile ../../node_config &
cd ../../

cd log/ns2
nohup java -ea -cp $gns_jar edu.umass.cs.gns.main.StartNameServer -multipaxos -id 2 -nsfile ../../node_config &
cd ../../


# start local name server
cd log/lns3
nohup java -ea -cp $gns_jar edu.umass.cs.gns.main.StartLocalNameServer -id 3 -nsfile ../../node_config -experimentMode &
cd ../../

# sleep because multipaxos takes time to wake up
sleep 10

cd log/client
java -ea -cp $gns_jar edu.umass.cs.gns.test.nioclient.ClientSample ../../node_config $client_port
cd ../../
