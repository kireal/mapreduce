#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Mapper step of rejection process
#
#
# 30.08.2013
#
import sys,os,getopt,optparse,string,commands
def mapper(params):
	# ================= PARAMETERS SECTION ========================
	Delimeter = params[0]
	SelectIDX = params[1]
	# ================= PARSING SECTION ========================
	if len(SelectIDX)>0:
		print len(SelectIDX)
		for line in sys.stdin:
			value = []
			line = line.strip()
			split_line = line.split(Delimeter) # split record
			value = [split_line[int(i)] for i in SelectIDX.split(",")] # get velue from record
			print Delimeter.join(value)
	else:
		for line in sys.stdin:
			value = []
			line = line.strip()
			value = line.split(Delimeter) # split record			
			print Delimeter.join(value)
Delimeter = ";" #delimeter in data set
SelectIDX = "" # "" means select all column from dataset
params = [[]]
try:
	Delimeter = os.environ['Delimeter']
except:
	pass
try:
	SelectIDX = os.environ['SelectIDX']
except:
	pass
params = [Delimeter, SelectIDX]
mapper(params)