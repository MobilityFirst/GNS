#!/usr/bin/env python
import os
import sys
from parse import resp_values
def main():
    values = []
    files = os.listdir(sys.argv[1])
    for f in files:
        if os.path.isdir(sys.argv[1] + '/' + f) and f.startswith('log_ns_'):
            
            filename = sys.argv[1] + '/' + f + '/gnrs.xml.0'
            if os.path.exists(filename + '.gz'):
                os.system('gzip -d ' + filename + '.gz')
            
            v = resp_values(filename)
            v.append(f)
            values.append(v)
            
            #os.system('./parse.py ' + filename)
            
    from write_array_to_file import write_tuple_array
    write_tuple_array(values, 'ns_response_times.txt')
    
    
    


if __name__ == "__main__":
    main()
