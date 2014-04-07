

def read_keys_values(filename):
    f = open(filename)
    keys_values = {}
    for line in f:
        if line.startswith('#'):
            continue
        tokens = line.split()
        if len(tokens) == 2:
            keys_values[tokens[0]] = tokens[1]
    return keys_values
