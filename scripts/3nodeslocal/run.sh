#
./name-server.py --id 0 --nsFile name-server-info  --dataStore MONGO --location --consoleOutputLevel INFO --fileLoggingLevel FINE &
./name-server.py --id 1 --nsFile name-server-info  --dataStore MONGO --location --consoleOutputLevel INFO --fileLoggingLevel FINE &
./name-server.py --id 2 --nsFile name-server-info  --dataStore MONGO --location --consoleOutputLevel INFO --fileLoggingLevel FINE &
./local-name-server.py --id 3 --nsFile name-server-info --runHttpServer --consoleOutputLevel INFO --fileLoggingLevel FINE &
