import os 
from fabric.api import *
from fabric.colors import *
import sys
#env.user = 'umass_nameservice'
#env.key_filename = '/home/rahul/.ssh/id_rsa_pl'
#install_dir = '/home/umass_nameservice/'
#Instantiating the default install directory to users home directory

def_install = '/home/'+env.user+'/'
env.colorize_errors=True


def config_check(param):
	if param in env.keys():
		print blue("config_check passed")	
	else:
		print red("config file is incomplete "+param+" is not set")	
		sys.exit(-1)	


@task
def phost():
	env.hosts = open('hosts.txt', 'r').readlines()
	print "the hosts are " , env.hosts


@task
@parallel
def install_gns(install_dir = def_install):
	config_check("gns_path")
	print "the install dir is", install_dir
	run_string = 'rsync -avz  '+ env.gns_path +'  '+ env.user+'@'+env.host_string+':'+install_dir
	result = os.system(run_string)
	if result != 0:
		print "gns copy failed for the host" , env.host_string
	else:
		print " gns copy sucessfull for the host" , env.host_string


#The following functions 
@task
@parallel
def install_mongo(install_dir = def_install):
	config_check("mongo_download")
	print "Initiating install_mongo routine"
	print "the install dir is",install_dir
	print "the mongo_download variable is ", env.mongo_download
	#print "the various hosts are " , env.hosts
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



@task
@parallel
def install_java(install_dir = def_install):
	config_check("java_download")
	print "Initiating install_java routine"
	print "the install dir is",install_dir
	print "java download link is",env.java_download
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
