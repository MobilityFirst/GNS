#!/usr/bin/python
import os, time

print "Executing scheme "+ str(scheme) + " on nodes "+str(numnodes)
print "executing " + str(reqsps)

cmd = mainDir+'/contextServiceScripts/VaryingNodesScript.py '+str(numnodes)
os.system(cmd)
time.sleep(2)
	  
cmd = mainDir+'/contextServiceScripts/ResetAll.py'
os.system(cmd)
time.sleep(10)


cmd = mainDir+'/mysqlScripts/Start.py'
os.system(cmd)
time.sleep(10)

cmd = mainDir+'/mysqlScripts/CreateDB.py'
os.system(cmd)
												                  
cmd = mainDir+'/contextServiceScripts/RunContextService.py '+str(scheme)+' '+str(numAttr)+' '+str(reqsps)+' '+str(ratio)+' '+str(numnodes)
															                      
os.system(cmd)
	                    
# sleep for 2 mins for system to start
time.sleep(70)
		    
# start update load
cmd = mainDir+'/contextServiceScripts/StartRequestLoad.py '+str(scheme)+' '+str(reqsps)+' '+str(ratio)+' '+str(numAttr)+' '+str(guids) +' '+str(numnodes)

os.system(cmd)
print "request load started"

# kill the last one.
cmd = mainDir+'/contextServiceScripts/KillProcesses.py'
os.system(cmd)
print "Java and hyperdex killed"


#cmd = mainDir+'/mongoShardScripts/KillMongo.py'
#os.system(cmd)

