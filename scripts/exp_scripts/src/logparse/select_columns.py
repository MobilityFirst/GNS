import os,sys

def main():
    format_file = sys.argv[1]
    output_filename = sys.argv[2]
    
    macros_filename =  'macro1.txt'
    from read_macros import read_macros
    macros = read_macros(macros_filename)
    
    
    from graph_format import read_graph_format
    format = read_graph_format(format_file)
    
    columns = []
    column_headers = []
    
    for format_col in format:
        col_values = extract_column_from_file(format_col[0],format_col[1])
        
        columns.append(col_values)
        column_headers.append(format_col[2])

    column_headers = get_names_for_macros(column_headers,macros)
    
    write_columns(output_filename,columns,column_headers)

def get_names_for_macros(values,macros):
    new_dict = []
    for value in values:
        if macros.has_key(value):
            new_dict.append(macros[value])
        else :
            new_dict.append(value)
    return new_dict


def write_columns(output_filename,column_array,column_headers):
    """
    This method writes column values adjacent to
    each other in the output_filename
    """
    fw = open(output_filename,'w')
    # write the header line
    for i in range(len(column_headers)):
        if i == len(column_headers) - 1:
            fw.write(column_headers[i]+'\n')
        else :
            fw.write(column_headers[i]+'\t')
    
    # write other lines
    for j in range(len(column_array[0])):
        if j == 0 : continue
        for i in range(len(column_array)):
            if i == len(column_array) - 1:
                fw.write(str(column_array[i][j])+'\n')
            else :
                fw.write(str(column_array[i][j])+'\t')
    fw.close()




def extract_column_from_file(filename, col_no, n = True):
    """
    This method extracts column values from filename,
    colums are numbered 0,1,2,3 ..
    """
    
    f = open(filename)
    col_values = []
    for line in f:
        tokens = line.strip().split()
        try:
            if len(tokens) <= col_no:
                if n == False:
                    col_values.append('?')
                else:
                    col_values.append(-1.0)
            else :
                if n == False:
                    col_values.append(tokens[col_no])
                else:
                    col_values.append(float(tokens[col_no]))
        except:
            print 'EXCEPTION:', line,
            pass
            
    return col_values

    
if __name__ == "__main__":
    main()
