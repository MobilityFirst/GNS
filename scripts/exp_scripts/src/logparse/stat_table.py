#!/usr/bin/env python
import os
import sys

def main():
    graph_format_file = sys.argv[1]
    output_table(graph_format_file)

def output_table(graph_format_file):
    folder_names_2d, stat_names, columns, rows = read_graph_format(graph_format_file)
    #print folder_names_2d, stat_name, columns, rows
    for stat_name in stat_names:
        table_values = read_table_values(folder_names_2d, stat_name, columns, rows)
        #print table_values
        
        output_folder = os.path.split(graph_format_file)[0] 
        output_file = os.path.join(output_folder, stat_name + '.txt')
        from write_array_to_file import write_tuple_array
        write_tuple_array(table_values, output_file)
        os.system('cat ' + output_file)
        write_gpt_file_and_plot(output_file, stat_name)

def write_gpt_file_and_plot(filename, stat_name):
    output_folder = os.path.split(filename)[0]
    file1 = os.path.split(filename)[1]
    #
    gpt_file = os.path.join(output_folder,stat_name)
    # write gpt file
    fw = open(gpt_file, 'w')
    fw.write('set terminal pdf\n')
    fw.write('set ylabel "' + stat_name + '"\n')
    fw.write('set yrange [0:]\n')
    fw.write('set xrange [0:]\n')
    fw.write('set xlabel "Load"\n')
    fw.write('set output "' + stat_name+ '.pdf"\n')
    fw.write('plot "' + file1 + '" u 1:2 w lp t columnhead\n')
    fw.close()
    #os.system('cat ' + gpt_file)
    # run gnu plot
    gpt_file1 = os.path.split(gpt_file)[1]
    os.system('cd ' + output_folder + ';gnuplot ' + gpt_file)
    
def read_graph_format(filename):
    #   ROWS: 1 ,2 3, 4, 5, 6
    #  columns: replicate-all, static3
    f = open(filename)
    lines = f.readlines()
    
    rowcount = int(lines[0])
    colcount = int(lines[1])
    rows = lines[2].strip().split()
    columns = lines[3].strip().split()
    stat_names = lines[4].strip().split()
    offset = 5
    folder_names_2d = []
    for i in range(rowcount):
        folder_names_2d.append(lines[offset : offset + colcount])
        offset += (colcount + 1)
    return folder_names_2d, stat_names, columns, rows

def read_table_values(folder_names_2d, stat_name, columns, rows):
    values = []
    
    tuple1 = ['x']
    tuple1.extend(columns)
    values.append(tuple1)
    
    for row,folder_list in zip(rows, folder_names_2d):
        tuple1 = [row]
        for f in folder_list:
            f = f.strip()
            tuple1.append(get_stat_from_folder(stat_name, f))
        values.append(tuple1)
    return values


def get_stat_from_folder(stat_name, folder):
    stat_files = ['summary.txt', 'latency_stats.txt','latency_stats_names.txt', 'ns-fairness.txt','avg_replica_count4.txt']
    files = []
    for f1 in stat_files:
        files.append(os.path.join(folder, f1))
    
    lines = []
    for f in files:
        if os.path.isfile(f):
            lines.extend(open(f).readlines())
    #print lines
    for line in lines:
        tokens = line.strip().split('\t')
        
        if tokens[0] == stat_name:
            return tokens[1]
    return -1

if __name__ == "__main__":
    main()

