#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Mapper step
#
# Kireal MapReduce Join Algorithm implementation
#	Simple python script to join to data set (files)
#	Reduce side implementation (map by key, tagging and loop output in reduce step)
#
# 20.08.2013
#

import sys,os,getopt,optparse,string,commands
for line in sys.stdin:
	line = line.strip()
	CommaCount = line.count(";") #Cheating! beasause hadoop streaming cannot set different mappers on different inputs!
	if CommaCount==1:
		try:
			split_txt = line.split(";")
			CountryCode = split_txt[1]
			CountryName = split_txt[0]
			print '%s\t%s,%s' %(CountryCode,"CN",CountryName) #CountryCode is a key, CN is a tag for Country data set, rest part of output is a value
		except:
			print "*** Error rows in file with parameters " + str(sys.argv[1:])
	elif CommaCount==2:
		try:
			split_txt = line.split(";")
			City = split_txt[0]
			CountryCode = split_txt[1]
			Population = split_txt[2]
			print '%s\t%s,%s,%s,%s' %(CountryCode,"CT",City,CountryCode,Population) #CountryCode is a key, CT is a tag for Cities data set, rest part of output is a value
		except:
			print "*** Error rows in file with parameters " + str(sys.argv[1:])
	else:
		print "*** Error rows in file with parameters " + str(sys.argv[1:])
