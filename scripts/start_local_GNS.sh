#
java -cp ../dist/GNS.jar edu.umass.cs.gns.main.StartLocalNameServer -id 1 -nsfile ../conf/singleNStest/node_config_1ns_1lns -primary 1 &
java -cp ../dist/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 0 -nsfile ../conf/singleNStest/node_config_1ns_1lns -singleNS &