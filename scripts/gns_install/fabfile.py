import os 
from fabric.api import *

#env.user = 'umass_nameservice'
#env.key_filename = '/home/rahul/.ssh/id_rsa_pl'
install_dir = '/home/umass_nameservice/'


def phost():
	thost = []
	fopen = open('host.txt' , 'r')
	for i in fopen:
		thost.append(i.strip('\n'))
	print " done populating the hosts", thost
	env.hosts = thost
	#return thost



@parallel
def install_gns():
    run_string = 'scp '+' /rahul_extra/GNS/trunk/dist/GNS.jar '+ env.user+'@'+env.host_string+':'+install_dir
    result = os.system(run_string)
    if result != 0:
    	print "gns copy failed for the host" , env.host_string
    else:
    	print " gns copy sucessfull for the host" , env.host_string


#The following functions 
@parallel
def install_mongo():
	print "Initiating install_mongo routine"
	with settings(warn_only=True):
		result = run("source ~/.bashrc && mongo -version")
		if result.return_code == 0:
			print "mongo already exists in PATH, no need to install"
		else:
			print "need to install"
			run_string = 'scp '+'  install.sh  '+ env.user+'@'+env.host_string+':'+install_dir
			presult = os.system(run_string) #The place where the install needs to be copied
			print "presult is ", presult
			run('bash ' + install_dir+'install.sh'+' mongodb http://downloads.mongodb.org/linux/mongodb-linux-i686-2.6.1.tgz '+install_dir)



#The following functions 
@parallel
def install_java():
	print "Initiating install_java routine"
	with settings(warn_only=True):
		result = run('java -version')
		if result.return_code == 0:
			print "java already exists in PATH, no need to install"
		else:
			print "need to install"
			run_string = 'scp '+'  install.sh  '+ env.user+'@'+env.host_string+':/home/umass_nameservice/'
			presult = os.system(run_string) #The place where the install needs to be copied
			print "presult is ", presult
			run('bash '+ install_dir+'install.sh'+'  jdk http://download.oracle.com/otn-pub/java/jdk/7u55-b13/jdk-7u55-linux-i586.tar.gz  '+install_dir)
