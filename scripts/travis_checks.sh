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
test_repeat=7 # Temporary change, replace with 10 ASAP

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
	if [ $wfile_line_count -ne 1 ] ; then
		echo "Invalid warning count file, exiting.."
		exit 1
	fi

# A count of the number of warnings we get in compiling sources
# The idea is to never exceed this as more commits are added

	javac_src_warning_count=`cat $wcount_file|head -n 1|tail -n 1`
fi

# Switch to the project root
cd ..
project_root=`pwd`

# Always start clean, otherwise some files won't be compiled
ant clean

ant wiki
# Check changed files
if [ ! -z "$TRAVIS_COMMIT_RANGE" ]; then
	echo $TRAVIS_COMMIT_RANGE
	CHANGED_FILES=$(git diff --name-only $TRAVIS_COMMIT_RANGE)
	echo "List of changed files:\n $CHANGED_FILES";
	# Changes in wiki/docs. Execute wiki task.
	num_changed=`echo $CHANGED_FILES | wc -l`;
	num_in_wiki=`echo $CHANGED_FILES | grep -io "wiki/"|wc -l`;
	echo "$num_in_wiki changes in wiki out of $num_changed";
	# If there are any changes in wiki, and this is not a PR, commit it to gh-pages
	if [ $num_in_wiki -gt 0 ] && [ "$TRAVIS_PULL_REQUEST" = "false" ]; then
		git clone -b gh-pages --single-branch https://github.com/MobilityFirst/GNS GNS_wiki
		rsync -av wiki/generated/_data/ GNS_wiki/_data/
		rsync -av wiki/generated/docs/ GNS_wiki/docs/
		rsync -av wiki/generated/_includes/ GNS_wiki/_includes
		cd GNS_wiki
		git add -A; git commit -m "Wiki update from Travis CI";
		git remote add wiki-remote https://${GIT_API_KEY}@github.com/MobilityFirst/GNS.git
		git push wiki-remote gh-pages
	fi

	# If no other files, we can stop.
	if [ $num_changed -eq $num_in_wiki ]; then
		echo "Exiting because changes are only in wiki";
		exit 0;
	fi
fi

# Captures:
# [javac] 1069 problems (1069 warnings) 
# or 
# [javac] 1070 problems (1 error, 1069 warnings)

result_javac=`ant compile_eclipse -lib ./lib | tee /dev/tty | grep -iE "[0-9]+ (errors?|warnings?)"`

if echo "$result_javac"|grep -qiE  "[0-9]+ warnings?"; then
	javac_wcount=`echo "$result_javac"|grep -iEo "[0-9]+ warnings?"|grep -iEo "[0-9]+"`
else
	javac_wcount="0";
fi


if echo "$result_javac"|grep -qiE  "[0-9]+ errors?"; then
	javac_ecount=`echo "$result_javac"|grep -iEo "[0-9]+ errors?"|grep -iEo "[0-9]+"`
else
	javac_ecount="0";
fi



echo $javac_wcount " java warning(s)"
echo $javac_ecount " java error(s)"

if [[ $* == *$update_option* ]]; then
	cd `echo $script_dir`
	echo "Writing counts to $wcount_file"
	echo "$javac_wcount" > `echo $wcount_file`
	echo "Warning count updated to $javac_wcount"
	exit 0
fi

if [ "$javac_ecount" -gt "0" ]; then
	echo "java error count has increased from 0 to $javac_ecount. You have introduced additional errors. Exiting with failure.."
	exit 1
fi

if [ "$javac_wcount" -gt "$javac_src_warning_count" ]; then
	echo "java warning count has increased from $javac_src_warning_count to $javac_wcount. You have introduced additional warnings. Exiting with failure.."
	exit 1
elif [ "$javac_wcount" -lt "$javac_src_warning_count" ]; then
	echo "Warning count has reduced, consider updating the warning count file using $update_option option"
fi


# Do a complete build and generate jars
ant

# Run ant test multiple times

for ((i=0; i<=$test_repeat; i++)); do
   ant test || { echo "Test $i failed, exiting.." ; exit 1; }
done
