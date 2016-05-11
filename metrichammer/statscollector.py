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
            
        # Process the metrics if there are any
        if len(self.metrics) > 0:
            print(self.metrics)
        
            
        
