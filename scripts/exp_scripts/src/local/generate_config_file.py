import os


def write_local_config_file(filename, num_ns, num_lns, latency=100):
    parent_folder = os.path.split(filename)[0]
    if parent_folder != '' and parent_folder != '.':
        os.system('mkdir -p ' + parent_folder)

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


