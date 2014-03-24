@echo off
start "MongoDB" C:\mongodb\bin\mongod.exe --dbpath c:\mongodb\data
start "Name Server" java -cp ..\dist\GNS.jar edu.umass.cs.gns.nsdesign.StartNameServer -id 0 -nsfile ..\resources\testCodeResources\singleNStest\node_config_1ns_1lns -primary 1
start "Local Name Server" java -cp ..\dist\GNS.jar edu.umass.cs.gns.main.StartLocalNameServer -id 1 -nsfile ..\resources\testCodeResources\singleNStest\node_config_1ns_1lns -primary 1