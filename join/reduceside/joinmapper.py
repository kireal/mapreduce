#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Mapper step of reusable join two datasets (files) 
#
# Kireal MapReduce Join Algorithm implementation
#	Simple python script to join to data set (files)
#	Reduce side implementation (map by key, tagging and loop output in reduce step)
#
# 30.08.2013
#
import sys,os,getopt,optparse,string,commands
def mapper(params):
	# ================= PARAMETERS SECTION ========================
	LeftDSDelim = params[0]
	RightDSDelim = params[1]
	DSColCounts = params[2].split(",")
	DSColLeftCounts = int(DSColCounts[0])
	DSColRightCounts = int(DSColCounts[1])
	SelectIDX = params[3]
	JoinKeyIDX = params[4]
	if SelectIDX.count(";")>0:
		SelectLeftIDX = SelectIDX.split(";")[0];
		SelectRightIDX = SelectIDX.split(";")[1];
	else:
		SelectLeftIDX = ""
		SelectRightIDX = ""
	Delimeter = ";"
	IntermKeyDelim = '\t'
	LeftDSKeyIDX = JoinKeyIDX.split(";")[0]
	RightDSKeyIDX = JoinKeyIDX.split(";")[1]
	# ================= PARSING SECTION ========================
	for line in sys.stdin:
		line = line.strip()
		LeftColCount = line.count(LeftDSDelim)+1 
		RightColCount = line.count(RightDSDelim)+1
		if DSColLeftCounts == LeftColCount: #input left dataset
			#process left
			Delim = LeftDSDelim
			DSKeyIDX = LeftDSKeyIDX
			SelectIDX = SelectLeftIDX
			tag = "L"
		elif DSColRightCounts == RightColCount: #input right dataset
			#process right
			Delim = RightDSDelim
			DSKeyIDX = RightDSKeyIDX
			SelectIDX = SelectRightIDX
			tag = "R"
		else: #unknown dataset or parsing error
			continue #raise Exception("Unknown data set"), unknown line, got to next
		split_line = line.split(Delim) # split record
		try:
			key = [split_line[int(i)] for i in DSKeyIDX.split(",")] # get key from record
			if len(SelectIDX)>0:
				value = [split_line[int(i)] for i in SelectIDX.split(",")] # get velue from record
			else:
				value = split_line
			value.insert(0,tag)
			print Delimeter.join(key) + IntermKeyDelim + Delimeter.join(value)
		except:
			print "Unknown dataset or cannot parse row"
LeftDSDelim = ";" #delimeter in left data set
RightDSDelim = ";" #delimeter in left data set
DSColCounts = "1, 1" #column count in each dataset
SelectIDX = "" # "" means select all column from dataset
JoinKeyIDX = "1; 1" #it means join by first column by default
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