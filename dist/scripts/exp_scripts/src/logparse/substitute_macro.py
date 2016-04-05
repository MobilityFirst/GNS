import os
import sys
                                                    
def substitute_macro(macro,new_line,input_file,output_file):
    f = open(input_file)
    fw = open(output_file,'w')
    for line in f:
        if line.strip()==macro:
            fw.write(new_line+'\n')
        else :
            fw.write(line)
    fw.close()
