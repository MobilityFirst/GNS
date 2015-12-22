#!/usr/bin/python
import os, time

lines = [line.strip() for line in open("conf/contextServiceConf/contextServiceNodeSetup.txt")]

for index in range(len(lines)):
	node = lines[index].split( )
	print node[1]
	print "Starting context service on "+node[1]
	cmd = 'nohup java -Xmx1024m -cp scripts/contextServiceScripts/context-nodoc-GNS.jar:dist/jars/GNS.jar edu.umass.cs.contextservice.examples.StartContextServiceNode '+node[0]+' &'
	#cmd = '/home/ayadav/runContextService.sh '+node[1]+' '+str(node[0])
	os.system(cmd)
time.sleep(5)
print "\n\n#######Context service ready###########\n\n"
