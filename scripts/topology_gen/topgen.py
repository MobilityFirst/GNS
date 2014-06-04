#!/bin/python
import random

node_count = 10



f = open('mux_config.ns', 'a')
start_string="set ns [new Simulator] \nsource tb_compat.tcl \n \n"
router_string="set router [$ns node] \ntb-set-node-os $router FEDORA15-STD \n \n"
f.write(start_string)
f.write(router_string)

for count in range(node_count):
	delay = random.randint(1,10) * 10  #Generating Random Delays Between Links
	setnode="set node"+str(count)+ " [$ns node]" + "\n"
	setos = "tb-set-node-os $node"+str(count)+" FEDORA15-STD" + "\n"
	setlink = "set link"+str(count)+" [$ns duplex-link $node"+str(count)+" $router 50Mb "+str(delay)+"ms DropTail] \n"
	setmux = "tb-set-multiplexed $link"+str(count)+" 1 \n"
	f.write(setnode)
	f.write(setos)
	f.write(setlink)
	f.write(setmux)

'''
i = 0
link_count = 0
while i < node_count:
	j = i + 1
	while j < node_count:
		link_string = "set link"+str(link_count)+" [$ns duplex-link $node"+str(i)+" $node"+str(j)+" 100Mb 0ms DropTail] \n"
		lan_string = "set lan"+str(link_count)+" [$ns make-lan $node"+str(i)+" $node"+str(j)+" 100Mb 0ms] \n"
		f.write(lan_string)
		j += 1
		link_count += 1
	i += 1


#Start Topology using Lan Experiments 
i = 0
node_string = ' '
while i < node_count:
	node_string += "$node"+str(i)+" "
	i += 1


lan_string = 'set lan0 [$ns make-lan' +'" '+node_string +' " ' +' 100Mb 0ms] '
f.write(lan_string)
'''


end_string = "\n $ns rtproto Static \n$ns run \n#netbuild-generated ns file ends. \n"
f.write(end_string)
f.close()