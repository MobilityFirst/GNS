#!/bin/python

node_count = 5

f = open('config.ns', 'a')
start_string="set ns [new Simulator] \nsource tb_compat.tcl \n \n"
f.write(start_string)


for count in range(node_count):
	setnode="set node"+str(count)+ " [$ns node]" + "\n"
	f.write(setnode)

i = 0
link_count = 0
while i < node_count:
	j = i + 1
	while j < node_count:
		link_string = "set link"+str(link_count)+" [$ns duplex-link $node"+str(i)+" $node"+str(j)+" 100Mb 0ms DropTail] \n"
		f.write(link_string)
		j += 1
		link_count += 1
	i += 1



end_string = "\n $ns rtproto Static \n$ns run \n#netbuild-generated ns file ends. \n"
f.write(end_string)
f.close()