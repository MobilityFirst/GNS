#!/opt/local/bin/gnuplot -persist
#
#    
#    	G N U P L O T
#    	Version 4.6 patchlevel 0    last modified 2012-03-04 
#    	Build System: Darwin x86_64
#    
#    	Copyright (C) 1986-1993, 1998, 2004, 2007-2012
#    	Thomas Williams, Colin Kelley and many others
#    
#    	gnuplot home:     http://www.gnuplot.info
#    	faq, bugs, etc:   type "help FAQ"
#    	immediate help:   type "help"  (plot window: hit 'h')
# set terminal aqua 0 title "Figure 0" size 846,594 font "Times-Roman,14" noenhanced solid
# set output
set terminal pdf  noenhanced font "Helvetica, 10.5"

unset clip points
set clip one
unset clip two
set bar 1.000000 front
set border 31 front linetype -1 linewidth 1.000
set timefmt z "%d/%m/%y,%H:%M"
set zdata 
set timefmt y "%d/%m/%y,%H:%M"
set ydata 
set timefmt x "%d/%m/%y,%H:%M"
set xdata 
set timefmt cb "%d/%m/%y,%H:%M"
set timefmt y2 "%d/%m/%y,%H:%M"
set y2data 
set timefmt x2 "%d/%m/%y,%H:%M"
set x2data 
set boxwidth
set style fill  empty border
set style rectangle back fc lt -3 fillstyle   solid 1.00 border lt -1


set dummy x,y
set format x "% g"
set format y "% g"
set format x2 "% g"
set format y2 "% g"
set format z "% g"
set format cb "% g"
set format r "% g"
set angles radians
unset grid

set key title ""
set key inside bottom vertical Right noreverse enhanced autotitles nobox
set key noinvert samplen 4 spacing 1 width 0 height 0 


unset label
unset arrow
set style increment default
unset style line
unset style arrow
set style histogram clustered gap 2 title  offset character 0, 0, 0
unset logscale
set logscale x 10
set offsets 0, 0, 0, 0
set pointsize 1

set encoding default
unset polar
unset parametric
unset decimalsign
set view 60, 30, 1, 1
set samples 100, 100
set isosamples 10, 10
set surface
unset contour
set clabel '%8.3g'
set mapping cartesian
set datafile separator whitespace
unset hidden3d
set cntrparam order 4
set cntrparam linear
set cntrparam levels auto 5
set cntrparam points 5
set size ratio 0 1,1
set origin 0,0
set style data points
set style function lines
set xzeroaxis linetype -2 linewidth 1.000
set yzeroaxis linetype -2 linewidth 1.000
set zzeroaxis linetype -2 linewidth 1.000
set x2zeroaxis linetype -2 linewidth 1.000
set y2zeroaxis linetype -2 linewidth 1.000
set ticslevel 0.5
set mxtics default
set mytics default
set mztics default
set mx2tics default
set my2tics default
set mcbtics default
set xtics border in scale 1,0.5 mirror norotate  offset character 0, 0, 0 
set xtics autofreq  norangelimit
set ytics border in scale 1,0.5 mirror norotate  offset character 0, 0, 0 
set ytics autofreq  norangelimit
set ztics border in scale 1,0.5 nomirror norotate  offset character 0, 0, 0 
set ztics autofreq  norangelimit
set nox2tics
set noy2tics
set cbtics border in scale 1,0.5 mirror norotate  offset character 0, 0, 0 
set cbtics autofreq  norangelimit

set title "Latency CDF" 
set title  offset character 0, 0, 0 font "" norotate
set timestamp bottom 
set timestamp "" 
set timestamp  offset character 0, 0, 0 font "" norotate
set rrange [ * : * ] noreverse nowriteback
set trange [ * : * ] noreverse nowriteback
set urange [ * : * ] noreverse nowriteback
set vrange [ * : * ] noreverse nowriteback
set xlabel "Latency (ms)" 
set xlabel  offset character 0, 0, 0 font "" textcolor lt -1 norotate
set x2label "" 
set x2label  offset character 0, 0, 0 font "" textcolor lt -1 norotate
set xrange [ * : * ] noreverse nowriteback
set x2range [ * : * ] noreverse nowriteback
set ylabel "CDF of Requests" 
set ylabel  offset character 0, 0, 0 font "" textcolor lt -1 rotate by -270
set y2label "" 
set y2label  offset character 0, 0, 0 font "" textcolor lt -1 rotate by -270
set yrange [ * : * ] noreverse nowriteback
set y2range [ * : * ] noreverse nowriteback
set zlabel "" 
set zlabel  offset character 0, 0, 0 font "" textcolor lt -1 norotate
set zrange [ * : * ] noreverse nowriteback
set cblabel "" 
set cblabel  offset character 0, 0, 0 font "" textcolor lt -1 rotate by -270
set cbrange [ * : * ] noreverse nowriteback
set zero 1e-08
set lmargin  -1
set bmargin  -1
set rmargin  -1
set tmargin  -1
set locale "en_US.UTF-8"
set pm3d explicit at s
set pm3d scansautomatic
set pm3d interpolate 1,1 flush begin noftriangles nohidden3d corners2color mean
set palette positive nops_allcF maxcolors 0 gamma 1.5 color model RGB 
set palette rgbformulae 7, 5, 15
set colorbox default
set colorbox vertical origin screen 0.9, 0.2, 0 size screen 0.05, 0.6, 0 front bdefault

set loadpath 
set fontpath 

set grid x
set grid y
set ytics 0.2
set key bottom
set fit noerrorvariables
set yrange[0:1]
set output "latencies_cdf.pdf"
plot "writelatencies_cdf.txt" u 2:1 w l lw 4 t "WRITES",  "readlatencies_cdf.txt" u 2:1 w l lw 4 t "READS";

set output "ping_vs_mean.pdf"
plot  "readlatencies_cdf.txt" u 2:1 w l lw 4 t "READS", "ping_latency.txt" u 2:1 w l lw 4 t "PINGS", "closest_ns_latency.txt" u 2:1 w l lw 4 t "Closest-NS";

#set output "results/set1/latencies_cdf.pdf"
#plot "results/set1/static3/latencies_cdf.txt" u 2:1 w l lw 4 t "STATIC-3",\
#"results/set1/beehive/latencies_cdf.txt" u 2:1 w l lw 4 t "BEEHIVE",\
#"results/set2/uniform/latencies_cdf.txt" u 2:1 w l lw 4 t "UNIFORM",\
#"results/set2/locality/latencies_cdf.txt" u 2:1 w l lw 4 t "LOCALITY"

#quit()
reset

set xlabel "Local Name Server IDs"
set ylabel "Latency (ms)"
set title "Latencies of queries from each LNS"
set style data histogram
set style fill solid
set xrange [0:]
set logscale y
set yrange [1:10000]


# All: read + write
set output "latencies_median.pdf"
plot "alllatencies_hostwise.txt" u 4 t "Median Latencies"

set output "latencies_max.pdf"
plot "alllatencies_hostwise.txt" u 3 t "Max Latencies"

set output "latencies_avg.pdf"
plot "alllatencies_hostwise.txt" u 5 t "Avg Latencies"


# Read
set output "read_latencies_median.pdf"
plot "readlatencies_hostwise.txt" u 4 t "Median Read Latencies"

set output "read_latencies_max.pdf"
plot "readlatencies_hostwise.txt" u 3 t "Max Read Latencies"

set output "read_latencies_avg.pdf"
plot "readlatencies_hostwise.txt" u 5 t "Avg Read Latencies"


# Write
set output "write_latencies_median.pdf"
plot "writelatencies_hostwise.txt" u 4 t "Median Write Latencies"

set output "write_latencies_max.pdf"
plot "writelatencies_hostwise.txt" u 3 t "Max Write Latencies"

set output "write_latencies_avg.pdf"
plot "writelatencies_hostwise.txt" u 5 t "Avg Write Latencies"

reset

set style data histograms
set xtics rotate
set ylabel "Number of Requests"
set style fill solid
set output "summary.pdf"
plot "summary.txt" u 2:xtic(1) notitle


reset


#set xrange [0:]
#set xlabel "Time of experiment (min)"


#set output "timeline-median.pdf"
#set ylabel "Mean Latency (ms)"
#plot "latency_by_time.txt" u ($1*5):5 w lp notitle;

#set output "timeline-mean.pdf"
#set ylabel "Median Latency (ms)"
#plot "latency_by_time.txt" u ($1*5):6 w lp notitle;


reset
quit()

set xlabel "Read latency (ms)"
set ylabel "Queries (ordered by \narrival times)"

#set xtics 0,100000
set output "start_end_times.pdf"
plot "start_end_times.txt" using ($0 - $0):0:($2-$1):($0 - $0) with vectors head filled lt 2 t "Read latency"


