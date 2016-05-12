# coding=utf-8

import logging
from multiprocessing import Process, Queue
import os
import sys
import time
import random
import traceback
import socket
import configobj


try:
    from setproctitle import getproctitle, setproctitle
except ImportError:
    setproctitle = None

# Path Fix
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../")))
    
class Client(Process):
    
    
    def __init__(self,config,threadid,q):

        Process.__init__(self)
        
        self.threadid = threadid
        self.q = q
        # Initialize Logging
        self.log = logging.getLogger('metrichammer')
        # Initialize Members
        #self.configfile = configfile
        self.config = config
        
        self.proto = 'tcp'
        self.host = self.config['server']['host']
        self.port = int(self.config['server']['port'])
        self.socket = None
        self.keepalive = 0
        self.keepaliveinterval = int(self.config['server']['keepaliveinterval'])
        self.timeout = float(self.config['server']['timeout'])
        self.flow_info = 0
        self.scope_id = 0
        self.metrics = []
        self.batch_size = int(self.config['server']['batchsize'])
        self.max_backlog_multiplier = 4
        self.trim_backlog_multiplier = 5
        self.namespace = self.config['server']['namespace']
        self.maxmetrics = int(self.config['server']['maxmetrics'])
        self.runs = int(self.config['server']['runs'])
        self.metriccount = 0
        # error logging throttling
        self.server_error_interval = float(120)
        self._errors = {}
        
        self.statsqueue = []

        # We do this weird process title swap around to get the sync manager
        # title correct for ps
        if setproctitle:
            oldproctitle = getproctitle()
            setproctitle('%s - SyncManager' % getproctitle())
        if setproctitle:
            setproctitle(oldproctitle)
            
    def run(self):
        
        # Include some randomness so all the clients are not connecting at once
        random.seed()
        waittime = random.randint(0,10)
        
        self.log.debug("Waiting %d seconds for client startup"%(waittime))
        
        time.sleep(waittime)
        
        starttime = time.time()

        try:
            
            for i in range(0,self.runs):
                
                random.seed()
                metric = ("metric_%d"%(random.randint(0,self.maxmetrics)))
                host = ("host_%d"%(self.threadid))
                
                random.seed()
                value = round(random.random(),2)
                
                metricline = ("%s.%s.%s %s %s\n"%(self.config['server']['namespace'],host,metric,value,str(int(time.time()))))
                
                self.metriccount += 1
                self.process(metricline)
        
        except KeyboardInterrupt:
            self.log.error("Keyboard interrupt for thread: %s"%(self.threadid))
            
        finally:
            self.log.debug("Exiting thread %s"%(self.threadid))
            
            endtime = time.time()
        
            totaltime = endtime - starttime
        
            self.statscollector({'metric':'totalmetrics','value':self.metriccount,'time':time.time()})
            self.statscollector({'metric':'totaltime','value':totaltime,'time':time.time()})
            # Never ever forget to flush!! 
            self.flushstats()

    def process(self, metric):
        """
        Process a metric by sending it to graphite
        """
        # Append the data to the array as a string
        self.metrics.append(str(metric))

        if len(self.metrics) >= self.batch_size:
            
            starttime = time.time()
            
            self._send()
            endtime = time.time()
            totaltime = endtime - starttime
            
            self.statscollector({'metric':'sendtime','value':totaltime,'time':time.time()})
            
    def statscollector(self,log):
        """
        Stats collection buffer
        """     
        self.statsqueue.append(log)
        
        if len(self.statsqueue) > 1000:
            self.flushstats()
         
    def flushstats(self):
        """ 
        Flush the statistics
        """ 
        self.q.put(self.statsqueue)
        self.statsqueue = []
        
    def flush(self):
        """Flush metrics in queue"""
        self._send()

    def _send_data(self, data):
        """
        Try to send all data in buffer.
        """
        try:
            self.socket.sendall(data.encode('utf-8'))
            self._reset_errors()
        except Exception as e:
            self._close()
            self._throttle_error("GraphiteHandler: Socket error trying reconnect. %s"%str(e))
            self._connect()
            try:
                self.socket.sendall(data.encode('utf-8'))
            except:
                return
            self._reset_errors()

    def _send(self):
        """
        Send data to graphite. Data that can not be sent will be queued.
        """
        # Check to see if we have a valid socket. If not, try to connect.
        try:
            try:
                if self.socket is None:
                    self.log.debug("GraphiteHandler: Socket is not connected. "
                                   "Reconnecting.")
                    self._connect()
                if self.socket is None:
                    self.log.debug("GraphiteHandler: Reconnect failed.")
                else:
                    self.log.debug("GraphiteHandler: sending data. %s"%(self.metrics))
                    # Send data to socket
                    self._send_data(''.join(self.metrics))
                    self.metrics = []
            except Exception:
                self._close()
                self._throttle_error("GraphiteHandler: Error sending metrics.")
                raise
        finally:
            if len(self.metrics) >= (
                    self.batch_size * self.max_backlog_multiplier):
                trim_offset = (self.batch_size *
                               self.trim_backlog_multiplier * -1)
                self.log.warn('GraphiteHandler: Trimming backlog. Removing' +
                              ' oldest %d and keeping newest %d metrics',
                              len(self.metrics) - abs(trim_offset),
                              abs(trim_offset))
                self.metrics = self.metrics[trim_offset:]
        

    def _connect(self):
        """
        Connect to the graphite server
        """
        if (self.proto == 'udp'):
            stream = socket.SOCK_DGRAM
        else:
            stream = socket.SOCK_STREAM

        if (self.proto[-1] == '4'):
            family = socket.AF_INET
            connection_struct = (self.host, self.port)
        elif (self.proto[-1] == '6'):
            family = socket.AF_INET6
            connection_struct = (self.host, self.port,
                                 self.flow_info, self.scope_id)
        else:
            connection_struct = (self.host, self.port)
            try:
                addrinfo = socket.getaddrinfo(self.host, self.port, 0, stream)
            except socket.gaierror as ex:
                self.log.error("GraphiteHandler: Error looking up graphite host"
                               " '%s' - %s",
                               self.host, ex)
                return
            if (len(addrinfo) > 0):
                family = addrinfo[0][0]
                if (family == socket.AF_INET6):
                    connection_struct = (self.host, self.port,
                                         self.flow_info, self.scope_id)
            else:
                family = socket.AF_INET

        # Create socket
        self.socket = socket.socket(family, stream)
        if self.socket is None:
            # Log Error
            self.log.error("GraphiteHandler: Unable to create socket.")
            # Close Socket
            self._close()
            return
        # Enable keepalives?
        #if self.proto != 'udp' and self.keepalive:
        #    self.log.error("GraphiteHandler: Setting socket keepalives...")
        #    self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        #    self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE,
        #                           self.keepaliveinterval)
        #    self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL,
        #                           self.keepaliveinterval)
        #    self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        # Set socket timeout
        self.socket.settimeout(self.timeout)
        # Connect to graphite server
        try:
            self.socket.connect(connection_struct)
            # Log
            self.log.debug("GraphiteHandler: Established connection to "
                           "graphite server %s:%d.",
                           self.host, self.port)
        except Exception as ex:
            # Log Error
            self._throttle_error("GraphiteHandler: Failed to connect to "
                                 "%s:%i. %s.", self.host, self.port, ex)
            # Close Socket
            self._close()
            return

    def _close(self):
        """
        Close the socket
        """
        if self.socket is not None:
            self.socket.close()
        self.socket = None
        
    def _throttle_error(self, msg, *args, **kwargs):
        """
        Avoids sending errors repeatedly. Waits at least
        `self.server_error_interval` seconds before sending the same error
        string to the error logging facility. If not enough time has passed,
        it calls `log.debug` instead
        Receives the same parameters as `Logger.error` an passes them on to the
        selected logging function, but ignores all parameters but the main
        message string when checking the last emission time.
        :returns: the return value of `Logger.debug` or `Logger.error`
        """
        now = time.time()
        if msg in self._errors:
            if ((now - self._errors[msg]) >=
                    self.server_error_interval):
                fn = self.log.error
                self._errors[msg] = now
            else:
                fn = self.log.debug
        else:
            self._errors[msg] = now
            fn = self.log.error

        return fn(msg, *args, **kwargs)

    def _reset_errors(self, msg=None):
        """
        Resets the logging throttle cache, so the next error is emitted
        regardless of the value in `self.server_error_interval`
        :param msg: if present, only this key is reset. Otherwise, the whole
            cache is cleaned.
        """
        if msg is not None and msg in self._errors:
            del self._errors[msg]
        else:
            self._errors = {}
            
    def __del__(self):
        """
        Destroy instance of the GraphiteHandler class
        """
        self._close()
       
       
 
if __name__ == '__main__':

    config = config = configobj.ConfigObj('../metrichammer.conf')

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
    q = Queue()
    
    c = Client(config,0,q)
    c.run()
    
    
    print("Test complete")
