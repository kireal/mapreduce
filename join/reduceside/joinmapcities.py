#!/usr/bin/python
# -*- coding: utf-8 -*-

# Kireal MapReduce Join Algorithm implementation
#	Simple python script to join to data set (files)
#	Reduce side implementation (map by key, tagging and loop output in reduce step)
#
# 18.08.2013

import sys,os,getopt,optparse,string,commands
for line in sys.stdin:
	try:
		line = line.strip()
		split_txt = line.split(";")
		City = split_txt[0]
		CountryCode = split_txt[1]
		Population = split_txt[2]
		print '%s\t%s;%s;%s;%s' %(CountryCode,"CT",City,CountryCode,Population) #CountryCode is a key, CT is a tag for Cities data set, rest part of output is a value
	except:
		print "*** Error rows in file with parameters " + str(sys.argv[1:])