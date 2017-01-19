#!/usr/bin/env bash

# travis_test branch:commit handle [”ant testMethod…”|file] [rep_times]
# travis_test branch:commit --status handle [--clean]

set -e # Important, the whole script logic is dependent on this


DEFAULT_COMMAND="ant test"
bold=$(tput bold)
normal=$(tput sgr0)

get_reponame () {
	local user=$(basename $(dirname $1))
	local repo=$(basename $1 .git)
	echo $user"/"$repo
}

show_help () {
	echo "
	$0 branch:commit-SHA1
	( handle ["command"] rep_times | --status handle [--clean])
	
	Ex: $0 master:abcde sample_test \"ant test\" 10
	    $0 master:abcde --status sample_test --clean
	"
}

check_branch_commit () {
	git check-ref-format --branch $1 > /dev/null 
	if ! [[ $2 =~ [a-f0-9]{5,40} ]]; then
    	echo "Invalid commit SHA-1"
    	show_help
    	exit 1
	fi
}

trigger_build(){
	body='{
"request": {
  "branch":"'"$1"'"
}}'

repo=`echo "$account_repo"|sed 's;/;%2F;'`
result=`curl -s -X POST \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -H "Travis-API-Version: 3" \
  -H "Authorization: token $2" \
  -d "$body" \
  "https://api.travis-ci.org/repo/$repo/requests"`
}

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $scriptdir
git_url=$(git ls-remote --get-url origin)
account_repo=$(get_reponame $git_url)
repo_root=$(git rev-parse --show-toplevel)

if [ -z $1 ] || [[ ! $1 =~ : ]] ; then
	echo "Invalid argument: $1"
	show_help
	exit 1
fi

if ! type "travis" > /dev/null; then
  	echo "Travis CLI Client not found, enter token manually"
  	read -p "Enter your Travis token" token
else
	token=`travis token|cut -f 5`
	echo "Your Travis token is $token"
fi

branch_n_commit=(${1//:/ })
target_branch=${branch_n_commit[0]}
target_commit=${branch_n_commit[1]}

check_branch_commit $target_branch $target_commit

echo "Repository    : ${bold}$account_repo ${normal}"
echo "Project root  : ${bold}$repo_root ${normal}"
echo "Target branch : ${bold}$target_branch${normal}"
echo "Target commit : ${bold}$target_commit${normal}"
short_commit=$(echo $target_commit|cut -c1-5)

if [[ $2 =~ ^--status$ ]] ; then
	
	if [ -z $3 ]; then
		echo "Error: Handle expected"
		show_help
		exit 1
	fi

	new_branch="travis_temp_""$short_commit""_""$3"
	echo "Fetching runs from $new_branch.."
	dirn="$repo_root""/logs_""$short_commit""_""$3"
	hist=`travis history -r $account_repo -b $new_branch -l 10000`
	output=`echo "$hist"|cut -f 1 -d ' '|cut -f 2 -d '#'`
	totalcount=`echo "$output"| sed '/^\s*$/d' | wc -l`
	echo "${bold}$totalcount${normal} test run(s) found"

	passcount=`echo "$hist"|grep passed|wc -l`
	if [ $passcount -ne 0 ]; then
		echo "${bold}$passcount${normal} successful"
	fi

	failcount=`echo "$hist"|grep failed|wc -l`
	if [ $failcount -ne 0 ]; then
		echo "${bold}$failcount${normal} failure(s)"
	fi

	errcount=`echo "$hist"|grep errored|wc -l`
	if [ $errcount -ne 0 ]; then
		echo "${bold}$errcount${normal} error/timeout(s)"
	fi
	

	startedcount=`echo "$hist"|grep started|wc -l`
	if [ $startedcount -ne 0 ]; then
		echo "${bold}$startedcount${normal} running"
	fi
	

	yetcount=`echo "$hist"|grep created|wc -l`
	if [ $yetcount -ne 0 ]; then
		echo "${bold}$yetcount${normal} yet to start"
	fi
	
	echo "Downloading errored/failed logs.."
	sleep 2
	output=`echo "$hist"|grep -iE "errored|failed"|cut -f 1 -d ' '|cut -f 2 -d '#'`
	totalcount=`echo "$output"|wc -l`
	currentcount="0"
	set +e
	while read testid
	do
		if [[  $testid =~ ^[0-9]+$ ]]; then
		  mkdir -p "$dirn"
		  nextcount=$(($currentcount+1))
		  perc=`echo $(( $nextcount * 100 / $totalcount ))`
		  echo -ne "  Downloading logs $nextcount of $totalcount ($perc%)\033[0K\r"
		  travis logs $testid -r $account_repo --no-interactive &> $dirn"/"$testid"_travis_"$3".log"
		  currentcount=$(($currentcount+1))
		fi
	done <<< "$(echo -e "$output")"

	if [ "$currentcount" -gt 0 ]; then
		echo -e "\nLogs downloaded to ${bold}$dirn${normal}"
	else
		echo -e "\nNo logs to download"
	fi

	if [[ $4 =~ ^--clean$ ]] ; then
		echo "Deleting local and remote branch $new_branch"
		set +e
		git branch -D $new_branch
		git push origin --delete $new_branch
		set -e
	fi

else
	handle=$2
	command=$3
	n=$4

	if [[ $command =~ ^[0-9]+$ ]] ; then
		n=$command
		command=$DEFAULT_COMMAND
	fi
	
	if ! [[ $n =~ ^[0-9]+$ ]] ; then
		echo "Error: Invaid rep_times"
		show_help
		exit 1
	fi

	current_branch=$(git rev-parse --abbrev-ref HEAD)
	new_branch="travis_temp_""$short_commit""_""$handle"
	git check-ref-format --branch $new_branch > /dev/null
	travis_template=`cat $scriptdir/travis_template.sh`
	command=`echo ${command}|tr '\n' "\\\n"`
	sh_content=`echo "$travis_template"|sed 's~COMMAND_HERE~'"$command"'~'`
	yml_content=`cat "$scriptdir/travis_template.yml"`
	
	git checkout $target_branch
	git pull origin $target_branch
	echo "Checking out new branch $new_branch"
	git checkout -b $new_branch $target_commit

	echo "$sh_content" > "$scriptdir/travis_checks.sh"
	echo "$yml_content" > "$repo_root/.travis.yml"
	echo "Successfully wrote Travis script"
	exit
	git add -A
	git commit -am "Temp commit from Travis script"
	git push origin $new_branch
	git checkout $current_branch

	times="1"
	while [[ $times -le $n ]]; do
		echo -ne "  Attempting to trigger build $times of $n \033[0K\r"
		trigger_build $new_branch $token
		if [[ ${result} == *"remaining"* ]];then
			sleep 2
	    	echo -ne "  Successful \033[0K\r"
	    	sleep 2
	    	echo "  Trigger $times completed"
	    	times=$((times+1))
	    elif [[ ${result} == *"request_limit_reached"* ]]; then
			echo -ne "  Hourly trigger limit exceeded, will retry in a while .. \033[0K\r"
			sleep 500
		else
			echo "An error occured, unable to parse response"
			echo "$result"
			exit 1
		fi
	done
fi



