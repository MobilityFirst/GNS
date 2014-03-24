__author__ = 'abhigyan'


import os
import sys

def main():
    write_ec2_config_file(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])


def write_ec2_config_file(pl_folder, pl_ns, pl_lns, ec2_folder):
    os.system('mkdir -p ' + ec2_folder)
    ## assuming files in pl_folder are named 1 - 200 and number of nodes in pl_ns and pl_lns is 100 each

    node_id_mapping = get_node_id_mapping(pl_ns, pl_lns)
    for node_id in node_id_mapping:
        input_file = os.path.join(pl_folder, node_id)
        output_file = os.path.join(ec2_folder, 'config_' + node_id_mapping[node_id])
        write_one_file(input_file, node_id_mapping, output_file)
        print node_id, 'config_' + node_id_mapping[node_id]




def get_node_id_mapping(pl_ns, pl_lns):
    node_id = {}
    count = 0
    for i, line in enumerate(open(pl_ns)):
        count +=1
        node_id[str(i)] = line.strip()

    for i, line in enumerate(open(pl_lns)):
        node_id[str(i + count)] = line.strip()
    return node_id



def write_one_file(input_file, node_id_mapping, output_file):
    f = open(input_file)
    fw = open(output_file, 'w')
    for line in f:
        tokens = line.split()
        tokens[2] =  node_id_mapping[tokens[2]]
        fw.write('\t'.join(tokens))
        fw.write('\n')
    fw.close()

main()