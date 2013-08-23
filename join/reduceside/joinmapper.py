#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Mapper step of reusable join two datasets (files) 
#
# Kireal MapReduce Join Algorithm implementation
#	Simple python script to join to data set (files)
#	Reduce side implementation (map by key, tagging and loop output in reduce step)
#
# 21.08.2013
#

import sys,os,getopt,optparse,string,commands

def mapper(params):
	# ================= PARAMETERS SECTION ========================
	LeftDSDelim = params[0]
	RightDSDelim = params[1]
	DSColCounts = params[2].split(",")
	SelectIDX = params[3]
	JoinKeyIDX = params[4]
	SelectLeftIDX = SelectIDX.split(";")[0];
	SelectRightIDX = SelectIDX.split(";")[1];
	IntermValDelim = ";"
	IntermKeyDelim = '\t'
	PrintLeftstr = '%s' + IntermKeyDelim + '%s' #first is key, second dataset tag
	PrintRightstr = '%s' + IntermKeyDelim + '%s' #first is key, second dataset tag
	for i in xrange(len(SelectLeftIDX)):
		PrintLeftstr = PrintLeftstr + IntermediateDelim + '%s'
	for i in xrange(len(SelectLeftIDX)):
		PrintRightstr = PrintLeftstr + IntermediateDelim + '%s'
	# ================= PARSING SECTION ========================
	for line in sys.stdin:
		line = line.strip()
		LeftColCount = line.count(LeftDSDelim) #Cheating! beasause hadoop streaming cannot set different mappers on different inputs!
		RightColCount = line.count(RightDSDelim) #Cheating! beasause hadoop streaming cannot set different mappers on different inputs!
		if DSColCounts = LeftColCount: #input left dataset
			#process left
			LeftKey = 
		elif DSColCounts = RightColCount: #input right dataset
			#process right
		else: #unknown dataset or parsing error





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

LeftDSDelim = ";" #delimeter in left data set
RightDSDelim = ";" #delimeter in left data set
DSColCounts = [10, 10] #column count in each dataset
SelectIDX = [-1,-1] #-1 means select all column from dataset
JoinKeyIDX = [1,1] #it means join by first column by default
params = [[]]
try:
	LeftDSDelim = os.environ['LeftDSDelim']
except:
	pass
try:
	RightDSDelim = os.environ['RightDSDelim']
except:
	pass
try:
	DSColCounts = os.environ['DSColCounts']
except:
	pass
try:
	SelectIDX = os.environ['SelectIDX']
except:
	pass
try:
	JoinKeyIDX = os.environ['JoinKeyIDX']
except:
	pass
params = [LeftDSDelim, RightDSDelim, DSColCounts, SelectIDX, JoinKeyIDX]
mapper(params)