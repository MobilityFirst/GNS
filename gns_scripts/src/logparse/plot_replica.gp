set terminal pdf  fsize 13

set style data histograms
set style fill solid
set xtics rotate
set grid y
set ylabel "Number of Replicas"
set logscale y

set output "replica_count.pdf"
plot "replica_count.txt" u ($2 + $3):xtic(1) notitle

