import os


def write_local_config_file(filename, num_ns, num_lns, latency=100, random_ids=None):
    parent_folder = os.path.split(filename)[0]
    if parent_folder != '' and parent_folder != '.':
        os.system('mkdir -p ' + parent_folder)
    port = 35000
    if random_ids is None:
        ns_ids = range(num_ns)
        lns_ids = range(num_ns, num_ns + num_lns)
    else:
        ns_ids, lns_ids = generate_node_ids(num_ns, num_lns, random_ids)

    fw = open(filename, 'w')
    localhost = 'local'   # '127.0.0.1'
    for i in ns_ids:
        values = [str(i), 'yes', localhost, str(port), str(latency), '0.0', '0.0']
        fw.write(' '.join(values) + '\n')
        port += 10

    for i in lns_ids:
        values = [str(i), 'no', localhost, str(port), str(latency), '0.0', '0.0']
        fw.write(' '.join(values) + '\n')
        port += 10
    fw.close()


def generate_node_ids(num_ns, num_lns, rand_seed):
    """ Generates node IDs for name servers, and local name servers by using given seed for random number generator
    """
    import random
    random.seed(rand_seed)
    ns_ids = []
    while len(ns_ids) != num_ns:
        node_id = random.randint(0, 1000000)
        if node_id not in ns_ids:
            ns_ids.append(node_id)
    lns_ids = []
    while len(lns_ids) != num_lns:
        node_id = random.randint(0, 1000000)
        if node_id not in lns_ids and node_id not in ns_ids:
            lns_ids.append(node_id)
    return ns_ids, lns_ids
