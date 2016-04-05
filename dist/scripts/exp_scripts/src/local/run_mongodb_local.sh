#!/bin/sh
echo "Running mongo db on ..."
mongobinFolder=$1
dbFolder=$2
port=$3  # if

mkdir -p $dbFolder
nohup mongod --smallfiles --dbpath $dbFolder --port $port > /tmp/mongo.out 2> /tmp/mongo.err &
# no journal option for mongod
#cat hosts_ns.txt | parallel ssh {}  "nohup /home/abhigyan/mongodb/bin/mongod --nojournal --dbpath /home/abhigyan/gnrs-db-mongodb/{} >/dev/null 2>/dev/null < /dev/null &"

echo "Done!"
