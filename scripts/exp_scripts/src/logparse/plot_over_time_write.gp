set terminal pdf font "Helvetica, 12"



#unset xtics

#set yrange[80:160]
set xrange[0:]

#set title "Graph 1"
set ylabel "LNS ID"
set xlabel "Time of failed write (s)"
set output "write.pdf"
plot "write-failed.txt" u ($1/1000):2 w p notitle;

#unset yrange
set xrange[0:]
#set title "Graph 3"
set ylabel "Name"
set xlabel "Time of failed write (s)"
set output "write-name.pdf"
plot "write-failed.txt" u ($1/1000):3 w p notitle;

#unset yrange
set xrange[0:]
#set title "Graph 3"
set ylabel "NS"
set xlabel "Time of failed write (s)"
set output "write-ns.pdf"
plot "write-failed.txt" u ($1/1000):4 w p notitle;

