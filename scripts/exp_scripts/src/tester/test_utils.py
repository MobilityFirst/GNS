import random

__author__ = 'abhigyan'


def gen_random_string(size):
    chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    s = ''
    while len(s) < size:
        s += chars[random.randint(0, len(chars) - 1)]
    return s


def get_new_group_members_str(node_ids, group_size):
    nodes_str = [str(node) for node in get_new_group_members(node_ids, group_size)]
    return ':'.join(nodes_str)


def get_new_group_members(node_ids, group_size):
    # assert 3 <= group_size <= len(node_ids)
    import random
    # hosts = range(num_ns)
    from copy import copy
    node_ids_copy = copy(node_ids)
    random.shuffle(node_ids_copy)
    return node_ids_copy[:group_size]


def get_line_count(filename):
    """ Returns the number of non-empty lines
    """
    lines = open(filename).readlines()
    count = 0
    for line in lines:
        if len(line.strip()) > 0:
            count += 1
    return count
