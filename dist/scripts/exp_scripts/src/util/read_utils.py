

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


def read_array_from_file(filename):

    f = open(filename)
    array2d = []
    for line in f:
        if len(line.strip()) == 0:
            continue
        tokens = line.strip().split()
        array2d.append(tokens)
    return array2d


def read_col_from_file(filename):

    f = open(filename)
    array1d = []
    for line in f:
        if len(line.strip()) == 0:
            continue
        tokens = line.strip().split()
        array1d.append(tokens[0])
    return array1d

def read_col_from_file2(filename, col_no):
    f = open(filename)
    array1d = []
    for line in f:
        if len(line.strip()) == 0:
            continue
        tokens = line.strip().split()
        array1d.append(tokens[col_no])
        #print tokens[col_no]
    return array1d

