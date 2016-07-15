#!/bin/bash
SCRIPTS="`dirname \"$0\"`"
$SCRIPTS/shutdown.sh 2>/dev/null
java -cp jars/GNS.jar edu.umass.cs.gnsserver.database.MongoRecords -clear
rm -rf derby.log paxos_logs reconfiguration_DB paxos_large_checkpoints
echo "DROP DATABASE paxos_logs;" | mysql --password=gnsRoot -u root 2>/dev/null
kill -9  $(ps aux | grep ActiveWorker | grep -v "grep ActiveWorker" | awk "{ print \$2 }")
kill -9 `ps -ef|grep LocalNameServer|grep -v grep|awk '{print $2}'`
