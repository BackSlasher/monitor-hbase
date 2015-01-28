#!/usr/bin/env python
# Collects data from hbase
import os
import json
import urllib2


hbase_region_port=60030
hbase_region_host='hb2.dynamicyield.com'
hbase_cluster_name='production'
hbase_server_name=os.popen('hostname -f').read().rstrip()

def get_json(path):
  return json.loads(urllib2.urlopen(("http://%s:%s/%s" % (hbase_region_host,hbase_region_port,path))).read())


def collect_server_data():
  jmx=get_json("jmx")
  tasks=get_json("rs-status?format=json")
  server_bean=filter(lambda x: x['name']=='hadoop:service=RegionServer,name=RegionServerStatistics', jmx['beans'])[0];
  # TODO remove?
  region_bean=filter(lambda x: x['name']=='hadoop:service=RegionServer,name=RegionServerDynamicStatistics', jmx['beans'])[0];

  base_hash = {
    'running-tasks': len(filter(lambda x: x['state']=='RUNNING',tasks)),
    'compactionQueueSize': server_bean['compactionQueueSize'],
    'flushQueueSize': server_bean['flushQueueSize'],
    'readReqeustsCount': server_bean['readRequestsCount'],
    'writeRequestsCount': server_bean['writeRequestsCount'],
    'regionCount': server_bean['regions'],
  }
  ret_hash={}
  for k in base_hash.keys():
    ret_hash["hbase.%s.%s.%s" % (hbase_cluster_name,hbase_server_name.replace('.','-'),k)]=base_hash[k]
  return ret_hash

def config(cluster_name,server_name,hbase_hostname,hbase_port):
  global hbase_region_port
  hbase_region_port=hbase_port
  global hbase_region_host
  hbase_region_host=hbase_hostname
  global hbase_cluster_name
  hbase_cluster_name=cluster_name
  global hbase_server_name
  hbase_server_name=server_name
