import os
import sys
import exp_config


config_file = 'local_config'


def write_local_config_file(filename, num_ns, num_lns):
    parent_folder = os.path.split(filename)[0]

    latency = 0
    port = 35000

    fw = open(filename, 'w')
    for i in range(num_ns):
        port += 10
        values = [str(i), 'yes', '127.0.0.1', str(port), str(latency), '0.0', '0.0']
        fw.write(' '.join(values) + '\n')

    for i in range(num_lns):
        port += 10
        values = [str(i + num_ns), 'no', '127.0.0.1', str(port), str(latency), '0.0', '0.0']
        fw.write(' '.join(values) + '\n')

    fw.close()


