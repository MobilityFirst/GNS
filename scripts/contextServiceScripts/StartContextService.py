import os, sys, time


configName = ''
mysqlUser  = ''
mysqlPassword = ''
# right now scripts can only work from gns top level dir
dirPrefix = 'scripts/contextServiceScripts'

def writeDBFile():
    nodeConfigFilePath = dirPrefix+'/'+configName+'/contextServiceConf/contextServiceNodeSetup.txt'
    lines = []
    with open(nodeConfigFilePath) as f:
        lines = f.readlines()
    f.close()
    
    dbFilePath = dirPrefix+'/'+configName+'/contextServiceConf/dbNodeSetup.txt'
    writef = open(dbFilePath, "w")
    curr = 0
    while(curr < len(lines)):
        writeStr = str(curr)+' 3306 contextDB'+str(curr)+' '+mysqlUser+' '+mysqlPassword+"\n"
        writef.write(writeStr)
        curr = curr +1
    
    writef.close()
    print "db file write with user given username and password complete\n"
        
        
def startCSNodes():
    cmd = 'pkill -9 -f context-nodoc-GNS.jar'
    os.system(cmd)
    time.sleep(2)
    print "Killed old context service"
    
    nodeConfigFilePath = dirPrefix+'/'+configName+'/contextServiceConf/contextServiceNodeSetup.txt'
    cmdPrefix = 'java -ea -cp '+dirPrefix+'/context-nodoc-GNS.jar:jars/GNS.jar edu.umass.cs.contextservice.nodeApp.StartContextServiceNode '+\
                    '-id'
    lines = []
    with open(nodeConfigFilePath) as f:
        lines = f.readlines()
    f.close()
    
    configDirPath = dirPrefix+'/'+configName+'/contextServiceConf'
    curr = 0
    while(curr < len(lines)):
        cmd = cmdPrefix+' '+str(curr)+' -csConfDir '+configDirPath +' & '
        print "starting context service "+cmd
        os.system(cmd)
        curr = curr + 1
        
#print "sys.argv[1] "+sys.argv[0]+" "+str(len(sys.argv))
if(len(sys.argv) == 1):
    configName = 'locationSingleNodeConf'
    print "using locationSingleNodeConf configuration and mysql username as password specified in locationSingleNodeConf/contextServiceConf/dbNodeSetup.txt"
elif(len(sys.argv) == 2):
    configName = sys.argv[1]
    print "using default mysql username as password specified in "+configName+"/contextServiceConf/dbNodeSetup.txt"
elif(len(sys.argv) == 3):
    print "Mysql username and password both are needed\n"

elif(len(sys.argv) == 4):
    configName = sys.argv[1]
    mysqlUser  = sys.argv[2]
    mysqlPassword = sys.argv[3]
    writeDBFile()
        
startCSNodes()
#time.sleep(5)
#print "context service started"