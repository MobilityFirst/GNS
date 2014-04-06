@echo off
start "MongoDB" C:\mongodb\bin\mongod.exe --dbpath c:\mongodb\data
start "Name Server" java -cp ..\dist\GNS.jar edu.umass.cs.gns.main.StartNameServer -id 0 -nsfile ..\conf\singleNStest\node_config_1ns_1lns -singleNS
start "Local Name Server" java -cp ..\dist\GNS.jar edu.umass.cs.gns.main.StartLocalNameServer -id 1 -nsfile ..\conf\singleNStest\node_config_1ns_1lns -primary 1