# coding=utf-8

import logging
from multiprocessing import Process, Queue
import os

import sys
import time


class statscollector(Process):
    
    def __init__(self,config,q):


        Process.__init__(self)
        
        self.q = q
        self.config = config
        # Initialize Logging
        self.log = logging.getLogger('metrichammer')
        
        self.metrics = []
        
    def run(self):
        
        self.log.debug("Starting stats collector")
        
        # wait a few seconds for the workers to get ahead
        time.sleep(2)
        
        sleepcount = 0
        while True:

            # Test to see if the queue is empty, should be pretty full most of the time
            if self.q.empty() == True:
                
                if sleepcount > 3:
                    
                    self.log.debug("Statistics collection thread terminating due to empty statistics queue, no work to do..bye")
                    break
                sleepcount += 1
                time.sleep(2)
                continue
            
            sleepcount = 0
            self.metrics.append(self.q.get())
         
         
        bucket = {}    
        # Process the metrics if there are any
        # Should be a list of lists of dictionaries, that we need to parse through
        # then we sort them into buckets
        if len(self.metrics) > 0:
            self.log.debug("Placing metrics into buckets")
            for ilist in self.metrics:
                for k in ilist:
                    metricname = k['metric']
                    metricvalue = k['value']
                    metrictime = k['time']
                   
                    if metricname not in bucket:
                        bucket[metricname] = []
                    
                    #self.log.debug("%s,%s"%(metricname,metricvalue))
                    varray = bucket[metricname] 
                    varray.append(str(metrictime) + " " + str(metricvalue))

                    bucket[metricname] = varray

            self.metrics = []
            
            # Now generate files for analysis
            self.log.debug("creating bucket data files for post run analyis")
            for key, value in bucket.iteritems():
                self.log.debug("Creating data file %s"%(key))
                with open(key + ".data","w") as f:
                    for i in value:
                        f.write(i.encode('utf-8'))
                        f.write("\n")
                    f.close()
                    
            
                
            
        
            
            
            #print(self.metrics)
            pass
                
            
        
