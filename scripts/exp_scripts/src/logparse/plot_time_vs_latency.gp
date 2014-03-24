set terminal pdf font "Helvetica, 12"

#set logscale x
#set logscale y
set yrange [0:]
set grid x
set grid y
set ylabel "Request Latency (s)"
set xlabel "Experiment time (s)"
set title "Latency over time"
set output "latency-time.pdf"
plot "latency-over-time.txt" u ($7/1000):($5/1000) w p notitle;
