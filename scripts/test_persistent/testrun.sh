#

cd /Users/westy/Documents/Code/GNS/scripts/test_persistent

./name-server.py --id 0 --nsFile name-server-info  --dataStore MONGO --location &
./name-server.py --id 1 --nsFile name-server-info  --dataStore MONGO --location &
./name-server.py --id 2 --nsFile name-server-info  --dataStore MONGO --location &
./name-server.py --id 3 --nsFile name-server-info  --dataStore MONGO --location &
./name-server.py --id 4 --nsFile name-server-info  --dataStore MONGO --location &
./name-server.py --id 5 --nsFile name-server-info  --dataStore MONGO --location &
./name-server.py --id 6 --nsFile name-server-info  --dataStore MONGO --location &
./name-server.py --id 7 --nsFile name-server-info  --dataStore MONGO --location &
./name-server.py --id 8 --nsFile name-server-info  --dataStore MONGO --location &
./name-server.py --id 9 --nsFile name-server-info  --dataStore MONGO --location &
./name-server.py --id 10 --nsFile name-server-info --dataStore MONGO --location &
./name-server.py --id 11 --nsFile name-server-info --dataStore MONGO --location &
./name-server.py --id 12 --nsFile name-server-info --dataStore MONGO --location &
./name-server.py --id 13 --nsFile name-server-info --dataStore MONGO --location &
./name-server.py --id 14 --nsFile name-server-info --dataStore MONGO --location &
./name-server.py --id 15 --nsFile name-server-info --dataStore MONGO --location &

./local-name-server.py --id 16 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 17 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 18 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 19 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 20 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 21 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 22 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 23 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 24 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 25 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 36 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 27 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 28 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 29 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 30 --nsFile name-server-info --runHttpServer &
./local-name-server.py --id 31 --nsFile name-server-info --runHttpServer &

# cd /Users/westy/Documents/Code/GNRS-westy

# java -classpath build:lib/* edu.umass.cs.gnrs.httpserver.HTTPClient

# java -classpath build:lib/* edu.umass.cs.gnrs.httpserver.GnrsHttpServer -nsfile scripts/name-server-info -lnsid 0 -local

# java -classpath build:lib/* edu.umass.cs.gnrs.StartStatus -nsfile scripts/name-server-info