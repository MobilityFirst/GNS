#!/bin/bash

sudo kill -9  $(ps aux | grep GNS.jar | awk '{print $2}')
java -cp ../dist/GNS.jar edu.umass.cs.gns.main.StartLocalNameServer -address 127.0.0.1 -port 24398 -nsfile ../conf/singleNStest/node_config_1ns_1lns -primary 1 &
java -cp ../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 127.0.0.1 -nsfile ../conf/singleNStest/node_config_1ns_1lns -singleNS &
#
#java -cp ../dist/GNS.jar edu.umass.cs.gns.main.StartLocalNameServer -address 127.0.0.1 -port 24398 -nsfile ../conf/singleNStest/node_config_1ns_1lns -primary 1 -dnsGnsOnly &
