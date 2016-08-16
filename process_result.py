#!/bin/python

import sys,os

def multiply(x,y):return x*y

def processFile(fname):
    fin = open(fname, "r")
    lat = None
    flag = False
    for line in fin:
        if "2nd run" in line:
            flag = True
        if "The average latency" in line and flag:
            lat = int(line[len("The average latency is "):-3])
    return lat

def main():
    guid = sys.argv[1]
    benign = sys.argv[2]
    rate = int(sys.argv[3])
    s = range(1,11)
    result = []
    for i in s:
        avg_latency = processFile("result/output-"+guid+"-"+benign+"-"+str(rate*i))
        if avg_latency is None:
            print "experiment failed"
            break
        result.append(avg_latency)
    print "X axis:", map(multiply, [rate*int(guid)]*len(s), s)
    print "Y axis:", result
            
if __name__ == "__main__":
    main()
