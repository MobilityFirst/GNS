#

cd /home/ec2-user/GNRS-westy/scripts

#cd /Users/westy/Documents/Code/GNRS-westy/scripts

./local-name-server.py --id 3 --nsFile name-server-info-3 &
./name-server.py --id 0 --nsFile name-server-info-0 &
./name-server.py --id 1 --nsFile name-server-info-1 &
./name-server.py --id 2 --nsFile name-server-info-2 &

# cd /Users/westy/Documents/Code/GNRS-westy

# java -classpath build:lib/* edu.umass.cs.gnrs.main.Client -nsfile scripts/name-server-info &