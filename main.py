#!/usr/bin/env python

# Manages other scripts
import time
import os
import hbase
import ConfigParser
import signal
import logging
import sys

def signal_handler(signal, frame):
  print 'SIGINT, stopping'
  global should_cont
  should_cont=False

logging.basicConfig(level=logging.INFO)

config = ConfigParser.RawConfigParser()
config.read('client.cfg')

use_statsd=config.getboolean('general','usestatsd')
c=None

# create statsd connection
if use_statsd:
  logging.info("using StatsD: %s:%s" % (config.get('statsd','host'),config.getint('statsd','port')))
  import statsd
  c = statsd.StatsClient(
    config.get('statsd','host'),
    config.getint('statsd','port'),
    prefix='hbase.%s' % config.get('general','cluster'),
    )

# connect to hbase
logging.info("using Hbase: %s with %s region and %s master" % (config.get('hbase','host'), config.getint('hbase','region_port'), config.getint('hbase','master_port')))
hbase.config(
  os.popen('hostname -f').read().rstrip(),
  config.get('hbase','host'),
  config.getint('hbase','region_port'),
  config.getint('hbase','master_port'),
  )

mode=config.get('general','mode')
logging.info("mode: %s" %(mode))
use_region=(mode=='both') or (mode=='region')
use_master=(mode=='both') or (mode=='master')
freq=config.getint('general','transmitfrequency')

should_cont=True
signal.signal(signal.SIGINT, signal_handler)

while should_cont:
  # TODO replace to statsd
  try:
    glob={}
    if use_region: glob=dict(glob.items() +  hbase.region_data().items())
    if use_master: glob=dict(glob.items() +  hbase.master_data().items())
    if use_statsd:
      for k in glob.keys():
        c.gauge(k,glob[k])
    logging.info("Reporting %s" %(glob))
  except Exception as e:
    logging.error("Unexpected error: %s" % e)
  if should_cont: time.sleep(freq)
