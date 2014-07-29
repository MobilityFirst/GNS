import os
import sys
import inspect
import argparse
__author__ = 'abhigyan'

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  # script directory
parent_folder = os.path.split(script_folder)[0]
sys.path.append(parent_folder)

# copies (using rsync): GNS.jar, env config file for remote node, and all scripts to remote folder

# local jar location
local_jar = '/Users/abhigyan/Documents/workspace/GNS/dist/GNS.jar'

# config file for remote node
config_file = os.path.join(parent_folder, 'resources', 'skuld_env.ini')
ssh_key = '/Users/abhigyan/.ssh/id_rsa'
user = 'abhigyan'
remote_host = 'skuld.cs.umass.edu'
remote_skuld_folder = '/home/abhigyan/gns/test_folder'

# config_file = os.path.join(parent_folder, 'resources', 'emulab.ini')
# ssh_key = '/Users/abhigyan/.ssh/id_rsa_pl'
# user = 'abhigyan'
# remote_host = 'users.emulab.net'
# remote_skuld_folder = '/proj/MobilityFirst/gns/scripts/'

print 'Making remote dir ... ' + remote_skuld_folder
os.system('ssh -i ' + ssh_key + ' ' + user + '@' + remote_host + ' " mkdir -p " ' + remote_skuld_folder)

print 'Syncing jar ... ' + local_jar
os.system('rsync -e "ssh  -i ' + ssh_key + ' " ' + local_jar + ' ' + user + '@' + remote_host + ':' + remote_skuld_folder)

print 'Syncing script folder ... ' + parent_folder
# folder containing all scripts
os.system('rsync -e "ssh  -i ' + ssh_key + ' " -r ' + parent_folder + ' ' + user + '@' + remote_host + ':' + remote_skuld_folder)

print 'Syncing env file ... ', config_file
# TODO we do not know how to supply a parameter to a unit test
remote_conf_file = os.path.join(remote_skuld_folder, 'src', 'resources', 'distributed_test_env.ini')
os.system('rsync -e "ssh  -i ' + ssh_key + ' " ' + config_file + ' ' + user + '@' + remote_host + ':' + remote_conf_file)
