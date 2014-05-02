import os

__author__ = 'abhigyan'

#
# user=$1
# ssh_key=$2
# pl_ns=$3
# mongo_path=$4
# db_folder=$5
# port=$6
#
# echo "Running mongo db on ..."
# cat $pl_ns
# echo 'Creating data folders ...'
# cat $pl_ns | parallel ssh -i $ssh_key $user@{} "mkdir -p $db_folder/{}"
#
# echo 'Running mongod process...'
#
# #cat $pl_ns | parallel ssh -i $ssh_key $user@{} "nohup mongod --nojournal --smallfiles --dbpath $db_folder/{} >/dev/null 2>/dev/null < /dev/null &"
# cat $pl_ns | parallel ssh -i $ssh_key $user@{} "nohup $mongo_path/mongod --smallfiles --port $port --dbpath $db_folder/{} >/dev/null 2>/dev/null < /dev/null &"
#
# echo 'Check if  mongod process are running ...'
# cat $pl_ns | parallel ssh -i $ssh_key $user@{} "ps aux | grep mongod"
# echo "Done!"


def run_mongodb(user, ssh_key, ns_ids, mongo_path, db_folder, port):
    cmd_filename = '/tmp/run_mongodb.sh'
    mongod_full_path = os.path.join(mongo_path, 'mongod')
    print 'mongod full path:', mongod_full_path
    fw = open(cmd_filename, 'w')
    for ns_id, host_name in ns_ids.items():
        node_data_folder = os.path.join(db_folder, 'node', str(ns_id))
        print 'Node data folder: ', node_data_folder
        cmd = 'ssh -i ' + ssh_key + ' -oStrictHostKeyChecking=no -oConnectTimeout=60  ' + user + '@' + host_name + \
              ' "mkdir -p ' + node_data_folder + '; nohup ' + mongod_full_path + ' --smallfiles --port ' + str(port) + \
              ' --dbpath ' + node_data_folder + '  >/dev/null 2>/dev/null < /dev/null & "'
        fw.write(cmd + '\n')
    fw.close()
    os.system('parallel -a ' + cmd_filename)


if __name__ == '__main__':
    ns_ids = {'0': 'compute-0-13', '1': 'compute-0-13', '2': 'compute-0-14'}
    run_mongodb('abhigyan', '/home/abhigyan/.ssh/id_rsa', ns_ids,
                '/home/abhigyan/mongodb-linux-x86_64-2.6.0/bin/', '/state/partition1/gns_mongodb_data/', 21324)
