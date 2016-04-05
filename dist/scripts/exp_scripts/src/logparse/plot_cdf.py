#!/usr/bin/env python
import os
import sys

from select_columns import extract_column_from_file
from stats import get_cdf
from write_array_to_file import write_tuple_array
from substitute_macro import substitute_macro

def main():
    graph_format_file = sys.argv[1]
    if not os.path.exists(graph_format_file):
        print 'Graph format file does not exist:', graph_format_file
        return 
    filenames, schemes, col_nos, pdf_filename, output_dir, template_file = \
               read_graph_parameters(graph_format_file)
    
    print filenames, schemes, col_nos, pdf_filename, output_dir, template_file
    get_cdf_and_plot(filenames, schemes, col_nos, pdf_filename, output_dir, template_file)
    

def main2():
    #filename = sys.argv[1]
    filenames = ['/home/abhigyan/gnrs/results/dec4/static3/group_by_name.txt',
                 '/home/abhigyan/gnrs/results/dec4/beehive-2/group_by_name.txt',
                 '/home/abhigyan/gnrs/results/dec4/locality/group_by_name.txt']
    schemes = ['STATIC-3', 'LOCALITY']
    col_no = 5
    pdf_filename = 'cdf_names_mean.pdf'
    output_dir = '/home/abhigyan/gnrs/results/dec4/comparison-100names/'
    template_file = 'template1.gpt'
    get_cdf_and_plot(filenames, schemes, col_no, pdf_filename, output_dir, template_file)


def read_graph_parameters(graph_format_file):
    output_dir = os.path.dirname(graph_format_file)
    filenames = []
    schemes = []
    col_nos = []
    template_file = 'template1.gpt'
    pdf_filename = ''
    f = open(graph_format_file)
    for line in f:
        tokens = line.split()
        if tokens[0] == 'PDF':
            pdf_filename = tokens[1]
        if tokens[0] == 'SCHEME':
            filenames.append(tokens[2])
            schemes.append(tokens[1])
            col_nos.append(int(tokens[3]))
    return filenames, schemes, col_nos, pdf_filename, output_dir, template_file


def get_cdf_and_plot(filenames, schemes, col_nos, pdf_filename, output_dir, template_file):
    
    output_files = []
    for col_no,filename in zip(col_nos, filenames):
        y = os.path.split(filename)
        output_filename = os.path.join(y[0], 'cdf_' + str(col_no) + '_' + y[1])
        #print output_filename
        try:
            write_cdf(filename, col_no, output_dir, output_filename)
        except:
            print 'Exception:',filename 
        output_files.append(output_filename)
    
    gpt_code =  gnuplot_code(output_files, schemes, os.path.join(output_dir, pdf_filename))
    #print gpt_code
    output_gpt = 'temp.gpt'
    substitute_macro('INSERTPLOTCODE', gpt_code, template_file, output_gpt)
    try:
        os.system('gnuplot ' + output_gpt)
    except:
        print 'ERROR: Gnuplot error'
    print 'PDF File:', os.path.join(output_dir, pdf_filename)
    os.system('rm ' + output_gpt)

    
def write_cdf(filename, col_no, output_dir, output_filename):
    values = extract_column_from_file(filename, col_no)
    #print values
    cdf_values = get_cdf(values)
    #print cdf_values
    write_tuple_array(cdf_values, output_filename, p = True)
    #os.system('cat ' + output_filename)

def gnuplot_code(output_files, schemes, pdf_file):
    s = 'set xlabel "Latency (ms)"\n'
    s += 'set ylabel "CDF"\n'
    #s += 'set logscale x 10\n'
    s += 'set yrange[0:1]\n'
    s += 'set ytics 0.2\n'
    s += 'set output "' + pdf_file + '"\n'
    s += 'plot '

    for filename, scheme in zip(output_files, schemes):
        s += ' "' + filename + '" u 2:1 w l lw 6 t "' + scheme + '",'
    s = s[:-1]

    s += ';\n'
    return s
        
if __name__ == "__main__":
    #print gnuplot_code(['a','x'],['b', 'y'])
    main()

        
