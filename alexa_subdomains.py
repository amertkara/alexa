#!/usr/bin/env python

import sys
import socket
import json
import time
import random
import signal
import urllib, urllib2
import threading
import Queue
import logging
from sqlalchemy import orm, create_engine, MetaData, Table, Column, Integer, String, DateTime, Float, ForeignKey, schema, types, func, desc
##################################################################
#    Subdomain parser for Alexa Top 1000
##################################################################
### MySQL Parameters ###
MHOST = 'localhost'
MUSER = 'root'
MPASSWORD = '123456'
MDBNAME = 'alexa'
LIMIT = 500
##################################################################

class Domain(object):
    pass

class Subdomain(object):
    pass
        
metadata = MetaData()

domain_table = Table('domain', metadata,
    Column('domain_id', Integer, primary_key=True),
    Column('domain_name', String(255)),
)        

subdomain_table = Table('subdomain', metadata,
    Column('subdomain_id', Integer, primary_key=True),
    Column('domain_id', Integer, ForeignKey('domain.domain_id')),
    Column('subdomain_label', String(255)),
    Column('subdomain_asis', String(255)),
)

orm.mapper(Domain, domain_table, properties={'subdomains':orm.relation(Subdomain, backref='domain')})
orm.mapper(Subdomain, subdomain_table) 

class MySQLController(object):
    '''MySQL Base Class'''
    
    engine = None
    metadata = None
    session = None
    sessionMaker = None  
    
    def __init__(self):
        '''Creates engine and prepares the session factory'''
        
        try:
            self.engine = create_engine('mysql+mysqldb://'+MUSER+':'+MPASSWORD+'@'+MHOST+'/'+MDBNAME)
            self.sessionMaker = orm.sessionmaker(bind=self.engine, autoflush=False, autocommit=False, expire_on_commit=False)
        except sqlalchemy.exc.OperationalError as e:
            sys.exit()        
                
        metadata.bind = self.engine
        metadata.create_all()
       
    def connect(self):
        self.session = orm.scoped_session(self.sessionMaker)

    def disconnect(self):
        self.session.close()

class MySQLUtility(MySQLController):
    '''MySQL Utility Class'''
    
    def __init__(self):
        #parent init
        MySQLController.__init__(self)
    
    def getDomainCount(self):
        self.connect()
        count = self.session.query(func.count(Domain.domain_id)).one()
        self.disconnect()
        
        return int(count[0])
    
    def getLastInsertDomainId(self):
        self.connect()
        domain_id = self.session.query(Subdomain.domain_id).order_by(desc(Subdomain.domain_id)).limit(1).one()
        self.disconnect()
        if(len(domain_id) > 0):
            return domain_id[0]
        else:
            return 0
        
class ThreadedMySQLReader(threading.Thread, MySQLController):
    '''Mysql Threadable Reader Class'''
    
    def __init__(self, offset_queue, request_queue):
        '''offset_queue holds the offsets for mysql select and request_queue holds the domains'''
        #parent init
        MySQLController.__init__(self)
        threading.Thread.__init__(self)
        #self.variable init
        self.offset_queue = offset_queue
        self.request_queue = request_queue
        #connect to the database and open session
        self.connect()
        
    def cleanDomainName(self, domainName):    
        '''strips the traversed domain names example.com/something'''
         
        #remove forward slashes
        fslashPos = domainName.find('/')
        if(fslashPos != -1):
            domainName = domainName[:fslashPos]
            
        return domainName
    
    def isIp(self, domainName):
        '''detects IP address'''
        
        try:
            socket.inet_aton(domainName)
            return True
        except socket.error:
            return False
    
    def read(self, offset, domainTableName):
        '''Reads table for feature calculation and returns the result list'''
        list = self.session.query(domainTableName).offset(offset).limit(LIMIT)
        return list
    
    def run(self):
        '''Thread Run'''
        while True:
            offset = self.offset_queue.get()
            domains = self.read(offset, Domain)
            for d in domains:
                if(not self.isIp(d.domain_name)):
                   d.domain_name = self.cleanDomainName(d.domain_name)
                   #push to the request queue
                   self.request_queue.put(d)
            self.offset_queue.task_done()
            
class ThreadedMySQLWriter(threading.Thread, MySQLController):
    """Mysql Writer Class for Channel 202"""
    
    def __init__(self, write_queue):
        #parent init
        MySQLController.__init__(self)
        threading.Thread.__init__(self)
        #self.variable init
        self.write_queue = write_queue
        #connect to the database and open session
        self.connect()
            
    def callback(self, subdomainList):
        """Custom Insert function for channel 202"""
            
        for subdomain in subdomainList:
            try:    
                self.session.add(subdomain)
                self.session.commit()
            except:
                return None
        
    def run(self):
        """Thread Run"""
        while True:
            subdomainList = self.write_queue.get()
            self.callback(subdomainList)
            self.write_queue.task_done()   
            
            
class ThreadedGoogleSearcher(threading.Thread, MySQLController):
    '''Threadable Google Search Class'''
    
    def __init__(self, request_queue, write_queue):
        '''request_queue holds the domain names to be searched and subdomains are pushed to the process_queue'''
        #parent init
        threading.Thread.__init__(self)
        MySQLController.__init__(self)
        #self.variable init
        self.request_queue = request_queue
        self.write_queue = write_queue
        self.proxies = []
        
        #logger init
        self.logger = logging.getLogger("ThreadedGoogleSearcher")
        self.logger.setLevel(logging.DEBUG)
        self.consoleHandler = logging.StreamHandler()
        self.consoleHandler.setLevel(logging.DEBUG)
        self.logger.addHandler(self.consoleHandler)
    
    def refreshProxyList (self):
        '''reads the proxies from a file named proxies in the same dir'''
        
        proxies_cache = self.proxies
        self.proxies = []
        
        try:
            f = open('./proxies', 'r')
            while True: 
                line = f.readline()
                if(len(line) > 0):
                    self.proxies.append(str(line).rstrip())
                else:
                    break        
        except:
            self.proxies = proxies_cache
            return None
        finally:
            f.close()
    
    def stripSubdomainLabel(self, subdomain, domain):
        '''strips out subdomain labels'''
        stripStart = subdomain.find('://')
        stripEnd = subdomain.find(domain)
        
        label = subdomain[stripStart+3:stripEnd-1]
        
        label = label.lstrip()
        label = label.rstrip()
        
        return label
        
    def searchSubdomains(self, domainName):
        '''searchs for the subdomain via google search api'''
        
        searchFor = '-inurl:www+site:'+domainName
        query = urllib.urlencode({'q': searchFor})
        searchUrl = 'http://ajax.googleapis.com/ajax/services/search/web?v=1.0&%s' % query
        
        request = urllib2.Request(searchUrl)
        request.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.15 (KHTML, like Gecko) Chrome/24.0.1295.0 Safari/537.15')
        socket.setdefaulttimeout(15)
        
        self.refreshProxyList()

        for i in range(len(self.proxies)):
            
            whichProxy = random.randrange(1, len(self.proxies))
            proxyAddr = self.proxies[whichProxy]
            self.logger.info('trying proxy '+proxyAddr)
            
            try:
                proxy = urllib2.ProxyHandler({'http':proxyAddr})
                opener = urllib2.build_opener(proxy)
                search_results = opener.open(request).read()
                results = json.loads(search_results)
                
            except KeyboardInterrupt:
                self.logger.info('maintanence mode; suspending for 1min')    
                time.sleep(60)
                self.refreshProxyList()
            except:
                self.logger.debug('some exception occured')
                continue
        
            data = results['responseData']
            
            if(data is not None and len(data['results']) > 0):
                return data['results']
            else:
                return None
    
    def run(self):
        '''Thread Run'''
            
        while True:
            domain = self.request_queue.get()
            subdomains = self.searchSubdomains(domain.domain_name)
            subdomainList = []
            if(subdomains is not None):
                for h in subdomains:
                    subdomain = Subdomain()
                    subdomain.domain_id = domain.domain_id
                    subdomain.subdomain_label = self.stripSubdomainLabel(h['url'], domain.domain_name)
                    subdomain.subdomain_asis = h['url']
                    subdomainList.append(subdomain)
                    
                self.logger.info('hit for '+domain.domain_name)
            else:
                self.logger.info('pushed back to the request queue '+domain.domain_name+'\n')
                self.request_queue.put(domain)
            
            self.write_queue.put(subdomainList)
            time.sleep(5)
            
            self.request_queue.task_done()

if __name__ == '__main__':
    
    #queues init
    offset_queue = Queue.Queue()
    request_queue = Queue.Queue()
    write_queue = Queue.Queue()
    
    #get Domain table total count
    mysqlUtility = MySQLUtility()
    #domainTableCount = mysqlUtility.getDomainCount()
    domainTableCount = 5000
    lastInsertedDomainId = mysqlUtility.getLastInsertDomainId()
 
    #prepare the offset_queue
    for i in range(lastInsertedDomainId,domainTableCount,LIMIT):
        offset_queue.put(i)
    
    #spawn read threads 
    for i in range(1):
        t = ThreadedMySQLReader(offset_queue, request_queue)
        t.setDaemon(True)
        t.start()
        
    #spawn search threads 
    for i in range(1):
        t = ThreadedGoogleSearcher(request_queue, write_queue)
        t.setDaemon(True)
        t.start()
        
        #spawn read threads 
    for i in range(1):
        t = ThreadedMySQLWriter(write_queue)
        t.setDaemon(True)
        t.start()    
    
    #wait until queues are emptied 
    offset_queue.join()
    request_queue.join()
    write_queue.join()