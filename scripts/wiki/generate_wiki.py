import json
import subprocess
import glob
import re

GIT_WIKI_PATH = 'https://github.com/akarthik10/GNS.wiki.git'
LOCAL_WIKI_PATH = 'wiki_temp'
KEY = 'constantName'
VALUE = 'constantValue'
CONSTANTS_FILE = 'wiki_constants.json'

with  open(CONSTANTS_FILE) as json_file:
	json_str = json_file.read()
	json_data = json.loads(json_str)

subprocess.call(['bash', '-c', '. get_wiki.sh '+GIT_WIKI_PATH+' '+LOCAL_WIKI_PATH+' ; cleanup; clone_wiki'])

data = {}
for item in json_data:
	data[item[KEY]] = item[VALUE]
json_data = data
pattern = re.compile(r'\b(' + '|'.join(json_data.keys()) + r')\b')

for filename in glob.glob('./'+LOCAL_WIKI_PATH+'/*.md'):
	with open(filename, 'r+') as wikifile:
		print "Writing "+ filename 
		result = pattern.sub(lambda x: json_data[x.group()], wikifile.read())
		wikifile.seek(0)
		wikifile.write(result)
		print "..done"

subprocess.call(['bash', '-c', '. get_wiki.sh '+GIT_WIKI_PATH+' '+LOCAL_WIKI_PATH+' ; commit_wiki; cleanup'])


