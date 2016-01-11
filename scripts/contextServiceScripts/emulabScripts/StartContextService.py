#!/usr/bin/python
import os, sys

#myList = []
#f = open("workingPlanetlab40.txt","r") #opens file with name of "test.txt"
#myList = f.readlines()
confName = sys.argv[1]

#scheme = sys.argv[1]
#numAttr = sys.argv[2]
#reqsps = sys.argv[3]
#ratio = sys.argv[4]
#numnodes = sys.argv[5]
#print "scheme running "+scheme

lines = [line.strip() for line in open("conf/"+confName+"/contextServiceConf/contextServiceNodeSetup.txt")]

#print(lines)
#for line in f:
#    myList.append(str(line))

#f.close()
# don't need to copy each time
#cmd = 'bash /Users/ayadav/Documents/GNS/ContextServiceExpScripts/scpContextServiceParallel.sh'
#os.system(cmd)

#cmd = 'bash /Users/ayadav/Documents/GNS/ContextServiceExpScripts/runContextServiceParallel.sh'
#os.system(cmd)
for index in range(len(lines)):
	node = lines[index].split( )
	print node[1]
	print "Starting context service on "+node[1]
	cmd = mainDir+'/contextServiceScripts/runContextService.sh '+node[1]+' '+str(node[0]) + ' '+str(scheme) + ' ' + str(numAttr)+ ' '+str(reqsps)+' '+str(ratio)+' '+str(numnodes)
	os.system(cmd)
