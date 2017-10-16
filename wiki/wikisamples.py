#!/usr/bin/python
import os
import sys
import re
import json


CONSTANTS_FILE = 'wiki_samples.json'
data = {}
count = 0

if len(sys.argv) == 1 :
	sys.argv[1:] = "."

for root, dirs, files in os.walk(sys.argv[1]):
	path = root.split(os.sep)
	for file in files:
		filename = root + os.sep + file
		# print filename
		with open(filename,'r') as open_file:
			content = open_file.read()
			tag = re.compile("\[@WIKI start (\w+)\]\s*(?:\s|(?:\*\/))?\s*(.*?)\s*(?:\/(?:\/|\*))?\s*\[@WIKI end \\1\]", re.DOTALL)
			match = tag.findall(content)
			if len(match) > 0 :
				count += len(match)
				for imatch in match:
					data[imatch[0]] = imatch[1]

__location__ = os.path.realpath(
	os.path.join(os.getcwd(), os.path.dirname(__file__)))
with open(os.path.join(__location__, CONSTANTS_FILE), 'w') as jsonfile:
	json.dump(data, jsonfile, indent = 4)
print "Wrote "+ str(count)+ " entries to "+ CONSTANTS_FILE