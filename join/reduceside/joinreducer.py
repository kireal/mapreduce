#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Reduce step
#
# Kireal MapReduce Join Algorithm implementation
#	Simple python script to join to data set (files)
#	Reduce side implementation (map by key, tagging and loop output in reduce step)
#
# 30.08.2013
#
import sys,os,getopt,optparse,string,commands
def Join (JoinType, LeftDS, RightDS, DelimeterL, DelimeterR):
	# -============== INNER JOIN PART ==============-
	if JoinType == "Inner":	#Inner
		for i in xrange(len(LeftDS)):
			if i == 0: # first elenent is null
				continue
			for j in xrange(len(RightDS)):
				if j == 0: # first elenent is null
					continue
				print DelimeterL.join(LeftDS[i]) + DelimeterR.join(RightDS[j])
	# -============== LEFT OUTER JOIN PART ==============-
	elif JoinType == "Left": #Left outer join
		for i in xrange(len(LeftDS)):
			if i == 0: # first elenent is null
				continue
			for j in xrange(len(RightDS)):
				if j == 0: # first elenent is null
					continue
			print DelimeterL.join(LeftDS[i]) + DelimeterR.join(RightDS[j])
	# -============== RIGHT OUTER JOIN PART ==============-	
	elif JoinType == "Right": #Rigth outer join
		for i in xrange(len(LeftDS)):
			if i == 0: # first element is null
				continue
			for j in xrange(len(RightDS)):
				if j == 0: # first element is null
					continue
			print DelimeterL.join(LeftDS[i]) + DelimeterR.join(RightDS[j])
	# -============== FULL OUTER JOIN PART ==============-			
	elif JoinType == "Full": #Full outer join
		for i in xrange(len(LeftDS)):
			if i == 0: # first element is null
				continue
			for j in xrange(len(RightDS)):
				if j == 0: # first element is null
					continue
			print DelimeterL.join(LeftDS[i]) + DelimeterR.join(RightDS[j])
	else:
		raise Exception("Unknown JoinType: " + JoinType)
def ReadStream (JoinType, DSColCounts, SelectIDX):
	Delimeter = ';'
	IntermValDelim = ';'
	IntermKeyDelim = '\t'
	LeftDS = [[]]
	RightDS = [[]]
	PrevKey = []
	iter = 0
	if len(JoinType) == 0:
		JoinType = "Inner"
	# -============== GENERATING EMPTY DS PART ==============-			
	if SelectIDX.count(";")>0:
		if len(SelectIDX.split(";")[0])>0:
			SelectLeftSize = SelectIDX.split(";")[0].count(',') + 1
		else:
			SelectLeftSize = DSColCounts.split(",")[0]
		if len(SelectIDX.split(";")[1])>0:
			SelectRightSize = SelectIDX.split(";")[1].count(',') + 1
		else:
			SelectRightSize = DSColCounts.split(",")[1]
	else:
		SelectLeftSize = DSColCounts.split(",")[0]
		SelectRightSize = DSColCounts.split(",")[1]
	LeftZeroDS = [Delimeter for x in range(int(SelectLeftSize))]
	RightZeroDS = [Delimeter for x in range(int(SelectRightSize))]
	DelimeterL = Delimeter
	DelimeterR = Delimeter
	# -============== READING STREAM PART ==============-		
	for line in sys.stdin:
			line = line.strip()
			split_txt = line.split(IntermKeyDelim) #extract key and value (by default \t is separator)
			key = split_txt[0]	#extract key
			if key != PrevKey:
				PrevKey = key
				if iter == 1:
					if len(LeftDS) == 1 and JoinType!="Inner":
						LeftDS.append(LeftZeroDS)
						DelimeterL = '' 
					if len(RightDS) == 1 and JoinType!="Inner":
						RightDS.append(RightZeroDS)	
						DelimeterR = ''				
					Join(JoinType, LeftDS, RightDS, DelimeterL, DelimeterR) #Join DSs
					LeftDS = [[]] #Init DS
					RightDS = [[]] #Init DS
				iter = 1
			value = split_txt[1].split(IntermValDelim) #extract value
			if value[0]=="L":
				LeftDS.append(value[1:])
			elif value[0]=="R":
				RightDS.append(value[1:])
	Join(JoinType, LeftDS, RightDS, Delimeter, SelectIDX) #Join DSs - last key
JoinType = "Inner"
DSColCounts = "1, 1" #column count in each dataset
SelectIDX = ""
try:
	JoinType = os.environ['JoinType']
except:
	pass
try:
	SelectIDX = os.environ['SelectIDX']
except:
	pass
try:
	DSColCounts = os.environ['DSColCounts']
except:
	pass
ReadStream(JoinType, DSColCounts, SelectIDX)