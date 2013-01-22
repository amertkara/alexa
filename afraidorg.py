#! /usr/bin/env python
# -*- coding: utf-8 -*-

from BeautifulSoup import BeautifulSoup
import urllib
import logging
import time
import MySQLdb as mysql
from string import Template

### MySQL Parameters ###
MHOST = "localhost"
MUSER = "root"
MPASSWORD = ""
MDBNAME = "afraidorg"

### Site-Specific Parameters ###
url = "http://freedns.afraid.org/domain/registry/"
urlParameters = "page-$count.html"
SLEEP = 10


class AfraidorgParser:
	
	pageCount = 1
	
	def readRemoteDocument(self, pageCount):
		finalUrl = self.url+self.urlParameters.substitute(count=pageCount)
		retryCount = 0
		for retryCount in xrange(10):
			try:
				self.logger.debug("Fetching remote document at " + finalUrl)
				docHandle = urllib.urlopen(finalUrl)
				doc = docHandle.read()
				return doc
			except IOError as e:
				self.logger.debug(e)
				continue
			break	
	
	def mineContent(self):
		#get the page count first
		self.totalPage = self.getTotalPageNumber()
		if(self.totalPage != None):
			for self.pageCount in xrange(self.pageCount, self.totalPage+1):
				tuples = []
				#sleep before each connection to mimic
				self.sleep()
				retryCount = 0
				for retryCount in xrange(10):
					try:
						doc = self.readRemoteDocument(self.pageCount)
						soup = BeautifulSoup(doc)
						rows = soup.center.table.findAll("tr")	 
						for row in rows:
							if(row.td.contents[0] != None and row.form == None and row.td.font == None):
								tuple = []
								domainName = row.contents[0].a.string.strip()
								numberOfHosts = int(self.chunkString(row.contents[0].span.contents[0].string.strip(), 1, " ").strip("()"))
								registerDate = self.chunkString(row.contents[3].string.strip(), 4, " ").strip("()")
								tuple = (domainName, numberOfHosts, registerDate)
								tuples.append(tuple)
					except AttributeError as e:
						self.logger.debug("broken document, retrying")
						continue
					break		 
				self.insertToMySQL(tuples)	  
		else:
			self.logger.debug("Coulnd't get the total page count, quitting")
			return
	
	def getTotalPageNumber(self):
		try:
			doc = self.readRemoteDocument(self.pageCount)
			soup = BeautifulSoup(doc)
			#get all trs, total page number is in the last tr
			rows = soup.center.table.findAll("tr")
			for row in rows:
				if(len(row.contents) > 2):
					if(row.contents[2].font != None and len(row.contents[2].font.contents) > 1):
						for str in row.contents[2].font.contents[2].split():
							if str.isdigit():
								self.logger.debug("Got the total page count: "+str)
								return int(str)
			return None
		except Exception as e:
			self.logger.debug(e)
		except TypeError as e:
			self.logger.debug(e)
	
	def chunkString(self, str, index, splitter):
		counter = 1
		for chunk in str.split(splitter):
			if(counter == index):
				return chunk
			counter += 1
	
	def connectToMySQL(self):
		try:
			self.conn = mysql.connect(MHOST, MUSER, MPASSWORD, MDBNAME)
		except Exception as e:
			self.logger.debug(e)
	
	def disconnectToMySQL(self):
		mysql.close(self.conn)		
	
	def insertToMySQL(self, tuples):
			for tuple in tuples:
				try:
					cursor = self.conn.cursor()
					cursor.execute("INSERT INTO domains (domain_name, host_count, register_date) VALUES (%s, %s, %s)", tuple)
					self.conn.commit()
				except mysql.Error as e:
					if(e.args[0] == 1062):
						self.logger.debug("duplicated entry detected, updating db with fresh data")
						self.updateMysql(tuple)
					else:
						self.logger.debug(e)
						
	def updateMysql(self, tuple):
		cursor = self.conn.cursor()
		cursor.execute("UPDATE domains SET host_count = %s, register_date = %s where domain_name=%s", (tuple[1], tuple[2], tuple[0]))
		self.conn.commit()		   
	
	def sleep(self):
		self.logger.debug("Sleeping for "+str(SLEEP)+" seconds");			 
		time.sleep(SLEEP)			
			
	def __init__(self, url, urlParameters):
		self.url = url
		self.urlParameters = Template(urlParameters)
		self.totalPage = 0
		self.pageCount = 1
		#logger
		self.logger = logging.getLogger("afraidorgparser")
		self.logger.setLevel(logging.DEBUG)
		self.consoleHandler = logging.StreamHandler()
		self.consoleHandler.setLevel(logging.DEBUG)
		self.logger.addHandler(self.consoleHandler)
	
	def main(self):
		self.connectToMySQL()
		self.mineContent()
	   

if __name__ == "__main__":
	parser = AfraidorgParser(url, urlParameters)
	parser.main()		