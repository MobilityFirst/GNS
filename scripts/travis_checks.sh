#!/usr/bin/env bash


# This script acts as a "status check" in Travis.
# This script will be run when a pull request is created and when something is committed.
# If this script fails, the pull request is flagged accordingly and cannot be merged.

# Usage travis_checks.sh [--update]
# --update will update warning counts instead of checking it


# If the script is invoked from a different directory,
# adjust pwd accordingly
cd $(dirname $0)
script_dir=`pwd`


# Run ant test 10 times
# A single failure will flag a Travis build failure
test_repeat=10

# The warning_counts file keeps track of previous warning counts
wcount_file="warning_counts"

# Run with --update to update warning counts
update_option=--update

# Warning count file should exist. Otherwise, run with --update
if [ ! -f  $wcount_file ] &&  [[ $* != *$update_option* ]]; then
    echo "warning count file not found, run the script with $update_option to reset counts. Exiting.."
    exit 1
fi


# If warning count file is present, ensure that it is sane
if [[ $* != *$update_option* ]]; then
	wfile_line_count=`cat $wcount_file|wc -l`
	if [ $wfile_line_count -ne 3 ] ; then
		echo "Invalid warning count file, exiting.."
		exit 1
	fi


# A count of the number of warnings we get in compiling sources
# The idea is to never exceed this as more commits are added

	javac_src_warning_count=`cat $wcount_file|head -n 1|tail -n 1`
	javadoc_src_err_count=`cat $wcount_file|head -n 2|tail -n 1`
	javadoc_src_warning_count=`cat $wcount_file|head -n 3|tail -n 1`
fi




# Switch to the project root
cd ..
project_root=`pwd`


ant clean

# Captures:
# [javac] 337 warnings
# [javac] 182 warnings
# First number is for src+test. Second is for test only.

result_javac=`ant | tee /dev/tty | grep -iE "[0-9]+ warnings"| grep -Eo "[0-9]+"`
num_lines=`echo "$result_javac"| wc -l`
if [ "$num_lines" -ne "2" ]; then
	echo "Unexpected output: $result_javac"
	exit 1
fi

javac_wcount=`echo "$result_javac"|head -n 1`

# Similar to the above result

result_javadoc=`ant doc| tee /dev/tty | grep -iE "[0-9]+ (errors|warnings)"`
num_lines=`echo "$result_javadoc"| wc -l`
if [ "$num_lines" -ne "2" ]; then
	echo "Unexpected output: $result_javadoc"
	exit 1
fi

javadoc_ecount=`echo "$result_javadoc"|grep -iE "[0-9]+ errors"|grep -Eo "[0-9]+"`
javadoc_wcount=`echo "$result_javadoc"|grep -iE "[0-9]+ warnings"|grep -Eo "[0-9]+"`



echo $javac_wcount " javac warnings"
echo $javadoc_ecount " javadoc errors"
echo $javadoc_wcount "javadoc warnings"

if [[ $* == *$update_option* ]]; then
	cd `echo $script_dir`
	echo "Writing counts to $wcount_file"
	echo "$javac_wcount" > `echo $wcount_file`
	echo "$javadoc_ecount" >> `echo $wcount_file`
	echo "$javadoc_wcount" >> `echo $wcount_file`
	echo "Warning counts updated"
	exit 0
fi

if [ "$javac_wcount" -gt "$javac_src_warning_count" ]; then
	echo "javac warning count has increased from $javac_src_warning_count to $javac_wcount. You have introduced additional warnings. Exiting with failure.."
	exit 1
fi

if [ "$javadoc_ecount" -gt "$javadoc_src_err_count" ]; then
	echo "javadoc error count has increased from $javadoc_src_err_count to $javadoc_ecount. You have introduced additional errors. Exiting with failure.."
	exit 1
fi

if [ "$javadoc_wcount" -gt "$javadoc_src_warning_count" ]; then
	echo "javadoc warning count has increased from $javadoc_src_warning_count to $javadoc_wcount. You have introduced additional warnings. Exiting with failure.."
	exit 1
fi

# Run ant test multiple times

for ((i=0; i<=$test_repeat; i++)); do
   ant test || { echo "Test $i failed, exiting.." ; exit 1; }
done
