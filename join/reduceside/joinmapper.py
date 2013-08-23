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
	LeftDSKeyIDX = JoinKeyIDX.split(";")[0]
	RightDSKeyIDX = JoinKeyIDX.split(";")[1]
#	for i in xrange(len(SelectLeftIDX)):
#		PrintLeftstr = PrintLeftstr + IntermediateDelim + '%s'
#	for i in xrange(len(SelectLeftIDX)):
#		PrintRightstr = PrintLeftstr + IntermediateDelim + '%s'
	# ================= PARSING SECTION ========================
	for line in sys.stdin:
		value = []
		key = []
		line = line.strip()
		LeftColCount = line.count(LeftDSDelim) #Cheating! beasause hadoop streaming cannot set different mappers on different inputs!
		RightColCount = line.count(RightDSDelim) #Cheating! beasause hadoop streaming cannot set different mappers on different inputs!
		if DSColCounts == LeftColCount: #input left dataset
			#process left
			split_line = line.split(LeftDSDelim) # split record
			key = [split_line[int(i)] for i in LeftDSKeyIDX.split(",")] # get key from record
			value = [split_line[int(i)] for i in SelectLeftIDX.split(",")] # get velue from record
			value.insert(0,"L")
		elif DSColCounts == RightColCount: #input right dataset
			#process right
			split_line = line.split(RightDSDelim) # split record
			key = [split_line[int(i)] for i in RightDSKeyIDX.split(",")] # get key from record
			value = [split_line[int(i)] for i in SelectRightIDX.split(",")] # get velue from record
			value.insert(0,"R")
		else: #unknown dataset or parsing error
			raise Exception("Unknown data set")
		print '\t'.join(key) + '\t' + ';'.join(value)

LeftDSDelim = ";" #delimeter in left data set
RightDSDelim = ";" #delimeter in left data set
DSColCounts = "10, 10" #column count in each dataset
SelectIDX = "-1;-1" #-1 means select all column from dataset
JoinKeyIDX = "1;1" #it means join by first column by default
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