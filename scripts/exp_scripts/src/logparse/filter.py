#!/usr/bin/env python
import os
import sys

def main():
    tuples_file = sys.argv[1]
    print_filtered(tuples_file)

def print_filtered(filename):
    lines = open(filename).readlines()
    for line in lines:
        tokens = line.split()
        if custom_filter(tokens):
            print tokens[4] + '\t' + tokens[6] + '\t' + tokens[7]

        

def custom_filter(tokens):
    if len(tokens) == 8 and tokens[2] == '20' \
            and tokens[5] == 'r':
        return True
    return False


if __name__ == "__main__":
    main()
