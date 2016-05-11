#!/usr/bin/env python
# coding=utf-8

import os
import sys
import configobj

from metrichammer.client import Client
from metrichammer.statscollector import statscollector

import optparse
import signal
import logging.config
from multiprocessing import Queue


try:
    from setproctitle import setproctitle
    setproctitle  # workaround for pyflakes issue #13
except ImportError:
    setproctitle = None

def main():
    
    try:
        parser = optparse.OptionParser()
        
        parser.add_option("-c", "--configfile",
                          dest="configfile",
                          default="/etc/metrichammer/metrichammer.conf",
                          help="config file")
        
        (options, args) = parser.parse_args()
        
        # Initialize Config
        if os.path.exists(options.configfile):
            config = configobj.ConfigObj(os.path.abspath(options.configfile))
            config['configfile'] = options.configfile
        else:
            sys.stderr.write("ERROR: Config file: %s does not exist." % str(options.configfile))

            sys.exit(1)
            
            
        # Initialize Logging
        log = logging.getLogger('metrichammer')

        log.setLevel(logging.DEBUG)
        # Configure Logging Format
        formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(message)s')
        # handler
        streamHandler = logging.StreamHandler(sys.stdout)
        streamHandler.setFormatter(formatter)
        streamHandler.setLevel(logging.DEBUG)
        log.addHandler(streamHandler)
    
    except Exception as e:
        import traceback
        sys.stderr.write("Unhandled exception: %s" % str(e))
        sys.stderr.write("traceback: %s" % traceback.format_exc())
        sys.exit(1)
        
    try:
        log.debug("Initializing Server")
        
        clients = []
        
        q = Queue()
        
        sc = statscollector(config,q)
        
        for c in range(0,int(config['server']['clients'])):
            log.debug("Initializing worker %s" %(c))
            
            client = Client(config,c,q)
            
            clients.append(client)
            
        # Start all the processes    
        for c in clients:
            
            c.start()
        
        # Start the statistics collector
        sc.start()
            
        # Wait for all the processes to finish
        for c in clients:
            
            c.join()
        
        # Wait for the statistics collector to finish
        sc.join()

        
    except Exception as e:
        import traceback
        log.error("Unhandled exception: %s" % str(e))
        log.error("traceback: %s" % traceback.format_exc())
        sys.exit(1)    
        

if __name__ == '__main__':
    if setproctitle:
        setproctitle('metrichammer')
    main()
        
