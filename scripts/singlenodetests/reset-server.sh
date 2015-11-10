#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
$SCRIPTS/killem.sh 2>/dev/null
java -cp jars/GNS.jar edu.umass.cs.gnsserver.database.MongoRecords -clear
rm -rf derby.log paxos_logs reconfiguration_DB paxos_large_checkpoints
echo "DROP DATABASE paxos_logs;" | mysql --password=gnsRoot -u root 2>/dev/null
