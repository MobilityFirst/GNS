echo "Running mongo db on ..."
dbFolder=/Users/abhigyan/Documents/gnrs-db/
mongobinFolder=/opt/local/bin/
port=27017  # if 

#/Users/abhigyan/Documents/mongodb-linux-x86_64-2.2.2/bin/
#mongodb-osx-x86_64-2.4.5/bin/
mkdir -p $dbFolder
nohup $mongobinFolder/mongod --smallfiles --dbpath $dbFolder --port $port &
# no journal option for mongod
#cat hosts_ns.txt | parallel ssh {}  "nohup /home/abhigyan/mongodb/bin/mongod --nojournal --dbpath /home/abhigyan/gnrs-db-mongodb/{} >/dev/null 2>/dev/null < /dev/null &"

echo "Done!"
