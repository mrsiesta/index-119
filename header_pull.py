#!/usr/bin/env python

import nntplib
import Queue
import sys
import threading
import yaml
from pprint import pprint
from elasticsearch import Elasticsearch
from elasticsearch import helpers

def get_helper(queue):
  while True:
    if queue.empty() == True:
      return 
    firstPost = queue.get()
    try:
      response = get_headers(firstPost, firstPost+articleIncrement)
      insert_headers(response)
    except Exception, e:
      print("error on " + fileName + " " + str(e))

def get_headers(firstPost, lastPost):
  esIndex = 'indextest'

  #get usenet creds
  credsFile = open("creds.yaml", 'r')
  creds = yaml.load(credsFile)  

  #setup connection to usenet server
  nntpServer = nntplib.NNTP(creds['host'], 119, creds['user'], creds['password'])  

  #set current group
  resp, count, first, last, name = nntpServer.group("alt.binaries.boneless")  

  #this is the data structure we're going to return
  finalHeaders = []  

  for postNumber in range(firstPost, lastPost):
    try: 
      header = nntpServer.head(str(postNumber))
      currentHeader = {
        '_index': esIndex,
        '_type': 'nntp',
        '_source': {
          'Article-Number': postNumber
        }
      }
      for entry in header[3]:
        splitString = entry.split(":",1)
        splitString[1] = splitString[1].lstrip()
        if splitString[0] == "Bytes" or splitString[0] == "Lines":
          currentHeader['_source'][splitString[0]] = int(splitString[1])
        if splitString[0] == "Newsgroups":
          currentHeader['_source'][splitString[0]] = splitString[1].split(",")
        else:
          currentHeader['_source'][splitString[0]] = unicode(splitString[1], "utf-8")  

      finalHeaders.append(currentHeader)
    except Exception as e:
      print str(e)
      print postNumber

  return finalHeaders

def insert_headers(headers):
  esHost = "localhost"
  esIndex = 'indextest'
  esIndexSettings = {
    "settings": {
      "number_of_shards": 5,
      "number_of_replicas": 0,
    }
  }

  print "inserting " + str(len(headers)) + " into ES"  

  es = Elasticsearch([esHost], sniff_on_start=True)
  es.indices.create(index=esIndex, body = esIndexSettings, ignore=400)  

  if len(headers) > 0:
    helpers.bulk(es, headers)

  return True


threads = 15
credsFile = open("creds.yaml", 'r')
creds = yaml.load(credsFile)
nntpServer = nntplib.NNTP(creds['host'], 119, creds['user'], creds['password'])  
resp, count, first, last, name = nntpServer.group("alt.binaries.boneless")  
articleIncrement = 100
print first
print last
workList = []
for blah in range(int(first),int(last),articleIncrement):
  workList.append(blah)

print len(workList)

queue = Queue.Queue()
activeThreads = []

for item in workList:
  queue.put(item)

for i in range(threads):
  t = threading.Thread(target=get_helper, args=(queue,))
  activeThreads.append(t)
  t.start()