
# Manages other scripts
import time
import os
import hbase
#import statsd
import ConfigParser
import signal

def signal_handler(signal, frame):
  print 'SIGINT, stopping'
  global should_cont
  should_cont=False

config = ConfigParser.RawConfigParser()
config.read('client.cfg')

# create statsd connection
#c = statsd.StatsClient('localhost', 8125, prefix='foo')

# connect to hbase
hbase.config(
  config.get('general','cluster'),
  os.popen('hostname -f').read().rstrip(),
  config.get('hbase','host'),
  config.getint('hbase','region_port'),
  config.getint('hbase','master_port'),
  )

mode=config.get('general','mode')
use_region=(mode=='both') or (mode=='region')
use_master=(mode=='both') or (mode=='master')
freq=config.getint('general','transmitfrequency')

should_cont=True
signal.signal(signal.SIGINT, signal_handler)

while should_cont:
  # TODO replace to statsd
  if use_region: print hbase.region_data()
  if use_master: print hbase.master_data()
  if should_cont: time.sleep(freq)
