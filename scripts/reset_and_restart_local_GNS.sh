#
../scripts/killem.sh
java -cp ../dist/GNS.jar edu.umass.cs.gnsserver.database.MongoRecords -clear
rm -rf log paxoslog log
../scripts/start_local_GNS.sh