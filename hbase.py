#!/usr/bin/env python
# Collects data from hbase
import os
import json
import urllib2
import contextlib


hbase_region_port=None
hbase_master_port=None
hbase_host=None
hbase_server_name=None

def get_json(path,port):
  url = "http://%s:%s/%s" % (hbase_host,port,path)
  with contextlib.closing(urllib2.urlopen(url)) as x:
      return json.loads(x.read())

# http://stackoverflow.com/a/20768199
def to_unsigned(original,bits=32):
    if original >= 0:
        return original
    else:
        return (original + 2**bits)

# Calculate current value. Take into account the possibility of overflow
# by using "unsigned" 32bit integers
def curr_rate(prev_value,curr_value):
    return to_unsigned(curr_value) - to_unsigned(prev_value)

prev_server_read=None
prev_server_write=None
def region_data():
  jmx=get_json("jmx",port=hbase_region_port)
  tasks=get_json("rs-status?format=json",port=hbase_region_port)
  server_bean=filter(lambda x: x['name']=='hadoop:service=RegionServer,name=RegionServerStatistics', jmx['beans'])[0];
  # TODO remove?
  region_bean=filter(lambda x: x['name']=='hadoop:service=RegionServer,name=RegionServerDynamicStatistics', jmx['beans'])[0];

  global prev_server_read
  read_rate=None
  curr_server_read = server_bean['readRequestsCount']
  if prev_server_read is not None:
      read_rate = curr_rate(prev_server_read, curr_server_read)
  prev_server_read = curr_server_read

  global prev_server_write
  write_rate=None
  curr_server_write = server_bean['writeRequestsCount']
  if prev_server_write is not None:
      write_rate = curr_rate(prev_server_write, curr_server_write)
  prev_server_write = curr_server_write

  base_hash = {
    'running-tasks': len(filter(lambda x: x['state']=='RUNNING',tasks)),
    'compactionQueueSize': server_bean['compactionQueueSize'],
    'flushQueueSize': server_bean['flushQueueSize'],
    'readRate': read_rate,
    'writeRate': write_rate,
    'regionCount': server_bean['regions'],
  }
  ret_hash={}
  for k in base_hash.keys():
    ret_hash["%s.%s" % (hbase_server_name.replace('.','-'),k)]=base_hash[k]
  return ret_hash


prev_region_read={}
prev_region_write={}
def master_data():
  jmx=get_json("jmx",port=hbase_master_port)
  tasks=get_json("master-status?format=json",port=hbase_master_port)
  region_bean=filter(lambda x: x['name']=='hadoop:service=Master,name=Master', jmx['beans'])[0]
  # count table regions
  # for every table region, create write requests, read requests
  region_count={}
  base_hash = {}


  for s in region_bean['RegionServers']:
    for r in s['value']['regionsLoad']:
      v=r['value']
      rawname=v['nameAsString'].encode('unicode-escape').split(',')
      tbl=rawname[0]
      reg=rawname[2].rstrip('.').replace('.','-')
      # increase region count
      if region_count.has_key(tbl):
        region_count[tbl]+=1
      else:
        region_count[tbl]=1
      # generate region stub
      region_name='%s.%s.readrequestcount' % (tbl,reg)

      global prev_region_read
      read_rate=None
      curr_read=v['readRequestsCount']
      if region_name in prev_region_read.keys():
          read_rate=curr_rate(curr_read, prev_region_read[region_name])
      prev_region_read[region_name]=curr_read
      base_hash['%s.readRate' % region_name]=read_rate

      global prev_region_write
      write_rate=None
      curr_write=v['writeRequestsCount']
      if region_name in prev_region_write.keys():
          write_rate=curr_rate(curr_write, prev_region_write[region_name])
      prev_region_write[region_name]=curr_write
      base_hash['%s.writeRate' % region_name]=write_rate

  for t in region_count.keys():
    base_hash['%s.regioncount' % 't']=region_count[t]

  ret_hash={}
  for k in base_hash.keys():
    ret_hash["tables.%s" % k]=base_hash[k]
  return ret_hash


def config(server_name=os.popen('hostname -f').read().rstrip(),hbase_hostname='127.0.0.1',region_port=60030,master_port=60010):
  global hbase_region_port
  hbase_region_port=region_port
  global hbase_master_port
  hbase_master_port=master_port
  global hbase_host
  hbase_host=hbase_hostname
  global hbase_server_name
  hbase_server_name=server_name
