#
./killem.sh
java -cp ../../dist/GNS.jar edu.umass.cs.gnsserver.database.MongoRecords -clear
rm -rf derby.log paxos_logs reconfiguration_DB paxos_large_checkpoints
./run-single.sh