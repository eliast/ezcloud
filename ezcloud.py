#!/usr/bin/python

import sys
import csv
import datetime
import httplib

try:
    import readline
except ImportError:
    print "Module readline not available."
else:
    import rlcompleter
    readline.parse_and_bind("tab: complete")

from time import mktime
from code import InteractiveConsole
from boto.ec2.connection import EC2Connection
from boto.ec2.elb import ELBConnection
from boto.ec2.cloudwatch import CloudWatchConnection

import atexit
import os
import readline
import rlcompleter

historyPath = os.path.expanduser("~/.ezcloud_history")

def save_history(historyPath=historyPath):
    import readline
    readline.write_history_file(historyPath)

if os.path.exists(historyPath):
    readline.read_history_file(historyPath)

atexit.register(save_history)

class GetResults(object):
  def __init__(self, ec2, responses):
    self.responses = responses
    self.ec2 = ec2
    
  def __repr__(self):
    return "\n".join([repr((r['status'], r['host'])) for r in self.responses])
  
  def expect(self, status=200):
    exceptions = filter(lambda r: r['status'] != status, self.responses)
    return InstanceList(self.ec2, [e['id'] for e in exceptions])
    
  def debug(self, status=None):
    if status:
      responses = filter(lambda r: r['status'] != status, self.responses)
    else:
      responses = self.responses

    for r in responses:
      print "(%(status)s) %(host)s" % r
      for header, value in r['headers'].items():
        print header, ":", value
      print r['body']
      print '-' * 40

class InstanceList(object):
  def __init__(self, ec2, ids):
    self.ec2 = ec2
    self.ids = list(ids)
    if not self.ec2._instances:
      self.ec2.refresh()
      
  def __getitem__(self, id):
    if isinstance(id, int):
      id = self.ids[id]
    return self.ec2._instances[id]
    
  def __repr__(self):
    return self.repr('short')
    
  def orderby(self, field):
    instances = [self.ec2._instances[id] for id in self.ids]
    instances.sort(lambda x,y: cmp(getattr(x,field), getattr(y, field)))
    self.ids = [i.id for i in instances]
    return self
    
  def dsh(self, name, user='root', base_dir="~/.dsh/group"):
    path = os.path.expanduser(base_dir)
    out = open(os.path.join(path, name), 'w')
    instances = (self.ec2._instances[id] for id in self.ids)
    if user:
      user = user + '@'
    else:
      user = ''
    for i in instances:
      line = "%s%s" % (user, i.public_dns_name)
      print line
      print >> out, line
    out.close()
  
  def get(self, path, port=80):
    instances = ((id, self.ec2._instances[id].public_dns_name) for id in self.ids)
    responses = []
    for id, host in instances:
      conn = httplib.HTTPConnection("%s:%d" % (host, port))
      conn.request('GET', path)
      r = conn.getresponse()
      #if '174' in host:
      #  responses.append({'status': 404, 'host':host, 'id': id, 'body': r.read(), 'headers': dict(r.getheaders())})
      #else:
      responses.append({'status': r.status, 'host':host, 'id': id, 'body': r.read(), 'headers': dict(r.getheaders())})
      
    return GetResults(self.ec2, responses)
    
  def repr(self, format):
    instances = (self.ec2._instances[id] for id in self.ids)
    return "\n".join(["\t".join([i.public_dns_name, i.placement, i.launch_time[:19], i.state, i.image_id]) for i in instances])

class EC2(object):
  def __init__(self, parent):
    self.parent = parent
    self.conn = EC2Connection()
    self.reservations = []
    self._instances = {}
    self.instances = None

  def __call__(self):
    return self.refresh()
        
  def refresh(self):
    self.reservations = self.conn.get_all_instances()
    for reservation in self.reservations:
      for instance in reservation.instances:
        self._instances[instance.id] = instance
    self.instances = InstanceList(self, self._instances.keys())
    return self
    
class MetricQuery(object):
  def __init__(self, metric):
    self.metric = metric
  def __call__(self, statistic=None, start=None, end=None, period=3600, unit=None, zone=None):
    if not end:
      end = datetime.datetime.now()
    if not start:
      start = end - datetime.timedelta(days=1)
    dimensions = self.metric.dimensions
    if zone:
      dimensions['AvailabilityZone'] = zone
    if not statistic:
      if self.metric.name == 'Latency':
        statistic = ['Average','Minimum','Maximum']
      else:
        statistic = ['Sum']
    if isinstance(statistic, str) or isinstance(statistic, unicode):
      statistic = [statistic]
    
    results = self.metric.connection.get_metric_statistics(period, start, end,
                                             self.metric.name, self.metric.namespace, statistic,
                                             dimensions, unit)
                  
    results.sort(lambda x,y: cmp(y['Timestamp'],x['Timestamp']))
    
    print 'Time', "\t".join([s for s in statistic])
    for result in results:
      print result['Timestamp'], "\t".join([str(result[s]) for s in statistic])

class Metrics(object):
  def __init__(self, name, metrics):
    self.metrics = metrics
    self.name = name
    for m in metrics:
      setattr(self, m.name.lower(), MetricQuery(m))
      
  def __repr__(self):
    return 'Metrics(%s):%r' % (self.name, [m.name.lower() for m in self.metrics])

class LoadBalancer(object):
  def __init__(self, parent, lb):
    self.parent = parent
    self.lb = lb
    self.instances = parent.parent.instance_list(lb.instances)
    self.cw_conn = parent.cw_conn
    self._metrics = None
    
  def __repr__(self):
    return repr(self.lb)
    
  def status(self):
    instances = self.parent.conn.describe_instance_health(self.lb.name)
    inservice = []
    outofservice = []
    for health in instances:
      instance_obj = self.instances[health.instance_id]
      #if '10' in health.instance_id:
      #  health.state = 'foo'
      #  health.description = 'wrong availability zone'
      if health.state == 'InService':
        inservice.append((instance_obj, health))
      else:
        outofservice.append((instance_obj, health))

    print "Load Balancer: ", self.lb.name       
    if not outofservice:
      print "Everything is OK."
    else: 
      print "%d instances out of service." % (len(outofservice),)
      for obj, health in outofservice:
        print obj.public_dns_name, health.description
        
  def metrics(self):
    if not self._metrics:
      self._metrics = Metrics(self.lb.name, [m for m in self.cw_conn.list_metrics() if 'LoadBalancerName' in m.dimensions and m.dimensions['LoadBalancerName'] == self.lb.name])
    return self._metrics

  def __getattr__(self, attr):
    return getattr(self.lb, attr)
  
class LoadBalancers(object):
  def __init__(self, parent):
    self.parent = parent
    self.conn = ELBConnection()
    self.lbs = []
    self.cw_conn = CloudWatchConnection()

  def __call__(self):
    self.lbs = [LoadBalancer(self, lb) for lb in self.conn.get_all_load_balancers()]
    return self
    
  def status(self):
    for lb in self.lbs:
      lb.status()
    
  def __getitem__(self, k):
    if isinstance(k, str) or isinstance(k, unicode):
      return [lb for lb in self.lbs if lb.lb.name == k].pop()
    else:
      return self.lbs[k]
    
  def __repr__(self):
    return 'LoadBalancers:' + ",".join([lb.name for lb in self.lbs])

class Cloud(object):
  def __init__(self):
    self.ec2 = EC2(self)
    self.lbs = LoadBalancers(self)
    
  def instance_list(self, instances):
    return InstanceList(self.ec2, (x.id for x in instances))

cloud = Cloud()

if __name__ == '__main__':

     banner = """Welcome to ezcloud!"""
     InteractiveConsole(globals()).interact(banner)
          
     del os, atexit, readline, rlcompleter, save_history, historyPath