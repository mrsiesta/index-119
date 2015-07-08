#!/usr/bin/env python

import nntplib
import Queue
import threading
import yaml
from elasticsearch import Elasticsearch
from elasticsearch import helpers

def get_helper(queue):
    while True:
        if queue.empty():
            return 
        first_post = queue.get()
        try:
            response = get_headers(first_post, first_post + article_increment)
            insert_headers(response)
        except Exception, e:
            print(str(e))

def get_headers(first_post, last_post):
    elastic_index = 'indextest'
  
    # get usenet creds
    creds_file = open("creds.yaml", 'r')
    creds = yaml.load(creds_file)  
  
    # setup connection to usenet server
    nntp_server = nntplib.NNTP(creds['host'], 119, creds['user'], creds['password'])  
  
    # set current group
    resp, count, first, last, name = nntp_server.group("alt.binaries.boneless")  
  
    # this is the data structure we're going to return
    final_headers = []  
  
    for post_number in range(first_post, last_post):
        try: 
            header = nntp_server.head(str(post_number))
            current_header = {'_index': elastic_index, '_type': 'nntp', '_source': {'Article-Number': post_number}}
            for entry in header[3]:
                split_string = entry.split(":",1)
                split_string[1] = split_string[1].lstrip()
                if split_string[0] == "Bytes" or split_string[0] == "Lines":
                    current_header['_source'][split_string[0]] = int(split_string[1])
                if split_string[0] == "Newsgroups":
                    current_header['_source'][split_string[0]] = split_string[1].split(",")
                else:
                    current_header['_source'][split_string[0]] = unicode(split_string[1], "utf-8")  
      
            final_headers.append(current_header)
        except Exception as e:
            print str(e)
            print post_number
  
    return final_headers

def insert_headers(headers):
    elastic_host = "localhost"
    elastic_index = 'indextest'
    elastic_index_settings = {
      "settings": {
        "number_of_shards": 5,
        "number_of_replicas": 0,
      }
    }
  
    elastic_mapping = {
      "nntp": {
        "properties": {
          "Subject": { 
            "type": "string",
            "index": "not_analyzed"
          }
        }
      }
    }

    print "inserting " + str(len(headers)) + " into ES"  
  
    es = Elasticsearch([elastic_host], sniff_on_start=True)
    es.indices.create(index=elastic_index, body=elastic_index_settings, ignore=400)  
    es.indices.put_mapping(index=elastic_index, doc_type="nntp", body=elastic_mapping)
  
    if len(headers) > 0:
      helpers.bulk(es, headers)
  
    return True



### MAIN
# TODO Cleanup - started resolving PEP-8 related
# TODO Add args for server, creds, article_increment, number of threads
# TODO default threads to number of cores -2
# TODO Add logging
threads = 2
creds_file = open("creds.yaml", 'r')
creds = yaml.load(creds_file)
nntp_server = nntplib.NNTP(creds['host'], 119, creds['user'], creds['password'])  
resp, count, first, last, name = nntp_server.group("alt.binaries.boneless")  
article_increment = 100
print first
print last
work_list = []
for blah in range(int(first),int(last),article_increment):
    work_list.append(blah)

print len(work_list)

queue = Queue.Queue()
active_threads = []

for item in work_list:
    queue.put(item)

for i in range(threads):
    thread = threading.Thread(target=get_helper, args=(queue,))
    active_threads.append(thread)
    thread.start()