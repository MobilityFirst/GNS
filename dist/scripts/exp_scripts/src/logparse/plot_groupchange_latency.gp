set terminal pdf font "Helvetica, 11"

set yrange [0:1]

#set logscale x
set xrange[0:]

set xlabel "Latency (ms)"
set ylabel "CDF of Group Changes"

set output "groupchange_cdf.pdf"
plot 'GroupChangeDuration_cdf' u 2:1 w l lw 8 t "GroupChange", "OldActiveStopDuration_cdf" u 2:1 w l lw 8 t "ActiveStop";

