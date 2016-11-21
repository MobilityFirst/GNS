#!/usr/bin/env bash

GIT_WIKI_PATH="$1"
LOCAL_WIKI_PATH="$2"

clone_wiki () {
	echo "Cloning repository.."
	git clone $GIT_WIKI_PATH $LOCAL_WIKI_PATH
}

cleanup () {
	echo "Cleaning up.."
	rm -rf $LOCAL_WIKI_PATH
}

commit_wiki () {
	echo "Committing.."
	cd $LOCAL_WIKI_PATH
	git add .
	git commit -m "Update at `date`"
	git push
	cd ..
}


