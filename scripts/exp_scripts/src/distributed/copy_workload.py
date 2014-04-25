import os

__author__ = 'abhigyan'


def copy_workload(user, ssh_key, lns_ids, trace_folder, remote_folder, remote_filename):
    """ Copies workload traces to respective local name servers. It assumes the name of the trace file for a local
    name is the same as the node ID of the local name server."""

    cmd_filename = '/tmp/copy_trace.sh'
    fw = open(cmd_filename, 'w')
    for lns_id, host_name in lns_ids.items():
        cmd = 'scp -i ' + ssh_key + ' -oStrictHostKeyChecking=no -oConnectTimeout=60 ' + trace_folder + '/' + str(lns_id) \
              + ' ' + user + '@' + host_name + ':' + remote_folder + '/' + remote_filename
        fw.write(cmd + '\n')
    fw.close()
    os.system('parallel -a ' + cmd_filename)
