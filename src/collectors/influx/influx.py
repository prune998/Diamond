# coding=utf-8
"""
Collect influxdb stats.

For the moment Influxdb does not share metrics
We only gather the number of series

#### Dependencies

 * subprocess
 * Influxdb client
 * urllib2

#### Example Configuration

InfluxdbCollector.conf

```
    enabled = True
    hosts = localhost:8086
    login = root
    password = root
    #database = 
```

"""

import diamond.collector
from influxdb import client as influxdb
import re


class InfluxCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(InfluxCollector, self).get_default_config_help()
        config_help.update({
            'login': "admin login to connect with. default to root",
            'password': "admin password. default to root",
            'hosts': "host and ports to collect. Set an alias by "
            + " prefixing the host:port with alias@",
            'database': "database to monitor. if empty, gather "
            + " stats for every database. Define more dbs as"
            + " database1,database2...",
        })
        return config_help
        
    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(InfluxCollector, self).get_default_config()
        config.update({
            'path':          'influxdb',

            # Connection settings
            'hosts':         ['localhost:8086'],
            'login':         "root",
            'password':      "root",
            'database':      None,
        })
        return config

    def connect(self, host, port, login, password):
        """ connect to influxdb """
        client = influxdb.InfluxDBClient(host, port, login, password)
        self.log.debug("connected to influxdb server on %s:%s as user %s", host, port, login)
        return client

    def get_stats(self, client, dbs):
        """ gather stats for each db """
        stats = {}

        for db in dbs:
          self.log.debug("gathering stats for DB %s", db)
          client.switch_db(db)
          series = client.query("list series")
          stats[db+".series.count"] = len (series[0]['points'])

        return stats

    def collect(self):
        hosts = self.config.get('hosts')

        # Convert a string config value to be an array
        if isinstance(hosts, basestring):
            hosts = [hosts]

        for host in hosts:
            matches = re.search('((.+)\@)?([^:]+)(:(\d+))?', host)
            alias = matches.group(2)
            hostname = matches.group(3)
            port = matches.group(5)

            client = self.connect(hostname, port, self.config.get('login'), self.config.get('password'))
            dbs=self.config.get('database')
            if dbs:
              if isinstance(dbs, basestring):
                dbs = [dbs]
            else:
              dblist = client.get_database_list()
              dbs=[]
              for db in dblist:
                dbs.append(db['name'])

            stats = self.get_stats(client,dbs)

            for stat in stats:
              if alias is not None:
                self.publish(alias + "." + stat, stats[stat])
              else:
                self.publish(stat, stats[stat])
                
