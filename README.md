# monitor-hbase
I use this tool for collecting performance metrics from HBase to Graphite, via StatsD.  
It works by polling HBase's web API for JMX data, extracts interesting values, and sends them to StatsD.

## Basic configuration
Configuration is done via `client.cfg`.  
Copy and customize `client.cfg.example`.

## Usage
Install the `statsd` pip package.  
Invoke `main.py`, like:
```python
python main.py
```

## Measured metrics
Assuming cluster name (defined in `client.cfg`) "CLUSTERNAME"

**Master servers**  
Assuming table "TABLENAME" and region "REGIONNAME"

* `CLUSTERNAME.tables.TABLENAME.regioncount`
* `CLUSTERNAME.tables.TABLENAME.REGIONNAME.readrequestcount`
* `CLUSTERNAME.tables.TABLENAME.REGIONNAME.writerequestcount`


**Region servers**  
Assuming server name (defaults to `hostname`) "SERVERNAME"

* `CLUSTERNAME.SERVERNAME.compactionQueueSize`
* `CLUSTERNAME.SERVERNAME.flushQueueSize`
* `CLUSTERNAME.SERVERNAME.readReqeustsCount`
* `CLUSTERNAME.SERVERNAME.regionCount`
* `CLUSTERNAME.SERVERNAME.running-tasks`
* `CLUSTERNAME.SERVERNAME.writeRequestsCount`
