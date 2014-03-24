set terminal pdf font "Helvetica, 12"





#set title "Graph 2"
set ylabel "LNS ID"
set xlabel "Time of failed read (s)"
set output "read.pdf"
plot "read-failed.txt" u ($1/1000):2 w p notitle;

#set title "Graph 4"
set ylabel "Name"
set xlabel "Time of failed read (s)"
set output "read-name.pdf"
plot "read-failed.txt" u ($1/1000):3 w p notitle;

#unset yrange
set xrange[0:]
#set title "Graph 3"
set ylabel "NS"
set xlabel "Time of failed read (s)"
set output "read-ns.pdf"
plot "read-failed.txt" u ($1/1000):4 w p notitle;

