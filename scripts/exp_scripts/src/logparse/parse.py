#!/usr/bin/env python
import os
import sys
#log_lns_147-179.surfsnel.dsl.internl.net/gnrs.xml.0:  <message>LNSListenerResponse: Received ResponseNum: 5 --&gt; {"sender":102,"Active":[18,25,40],"qname":"6","address":["-1042294418"],"ttlAddress":0,"Primary":[18,25,40],"qrecordkey":"edgeRecord","header":{"id":878924962,"rcode":0,"qr":1}}</message>
def main():
    f = open(sys.argv[1])
    output_file = 'response_times.txt'
    response_times = []
    except_count = 0
    for line in f:
        try:
            resp = get_response_time(line)
            response_times.append(resp)
        except:
            except_count += 1
            continue
    response_times.sort()
    avg = 0
    median = 0
    ninety = 0
    if len(response_times) > 0:
        avg = sum(response_times) / len(response_times)
        median = response_times[len(response_times)/2]
        ninety = response_times[len(response_times)*9/10]
    print len(response_times), avg, median, ninety
    
    from write_array_to_file import write_array
    write_array(response_times, output_file, p = True)
    return
    ids  = {}
    for line in f:
        j = line.index("id") + 4
        qid = line[j: j + 9]
        ids[qid] = 1
    print 'Number ids ', len(ids)
#log_ns_cs-planetlab3.cs.surrey.sfu.ca/gnrs.xml.0:  <message>NameServerLoadMonitor: responseTime:5</message>

def resp_values(filename):

    if os.path.exists(filename) == False:
        return [0, -1, -1, -1]
    f = open(filename)
    #output_file = 'response_times.txt'
    response_times = []
    except_count = 0
    for line in f:
        try:
            resp = get_response_time(line)
            response_times.append(resp)
        except:
            except_count += 1
            continue
    response_times.sort()
    avg = 0
    median = 0
    ninety = 0
    if len(response_times) > 0:
        avg = sum(response_times) / len(response_times)
        median = response_times[len(response_times)/2]
        ninety = response_times[len(response_times)*9/10]
    print len(response_times), avg, median, ninety
    return [len(response_times), avg, median, ninety]

def get_response_time(line):
    line = line[:-len('</message>') - 1]

    i = line.index('responseTime:') + len('responseTime:')

    response = int(line[i:])
    
    return response
    
if __name__ == "__main__":
    #get_response_time('log_ns_cs-planetlab3.cs.surrey.sfu.ca/gnrs.xml.0:  <message>NameServerLoadMonitor: responseTime:5</message>')
    
    main()
