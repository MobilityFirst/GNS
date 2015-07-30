#
./killem.sh
java -cp ../../dist/GNS.jar edu.umass.cs.gns.database.MongoRecords -clear
rm -rf derby.log paxos_logs reconfiguration_DB paxos_large_checkpoints
echo "DROP DATABASE paxos_logs;" | mysql --password=gnsRoot -u root
