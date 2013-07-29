#

cd /Users/westy/Documents/Code/GNRS-westy/scripts

./local-name-server.py --id 7 --nsFile name-server-info-7 &
./name-server.py --id 0 --nsFile name-server-info-0 &
./name-server.py --id 1 --nsFile name-server-info-1 &
./name-server.py --id 2 --nsFile name-server-info-2 &
./name-server.py --id 3 --nsFile name-server-info-3 &
./name-server.py --id 4 --nsFile name-server-info-4 &
./name-server.py --id 5 --nsFile name-server-info-5 &
./name-server.py --id 6 --nsFile name-server-info-6 &
./name-server.py --id 7 --nsFile name-server-info-7 &

# cd /Users/westy/Documents/Code/GNRS-westy

# java -classpath build:lib/* edu.umass.cs.gnrs.httpserver.HTTPClient

# java -classpath build:lib/* edu.umass.cs.gnrs.httpserver.GnrsHttpServer -nsfile scripts/name-server-info -lnsid 7 -local