#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Reduce step
#
# Kireal MapReduce Join Algorithm implementation
#	Simple python script to join to data set (files)
#	Reduce side implementation (map by key, tagging and loop output in reduce step)
#
# 01.09.2013
#
import sys,os,getopt,optparse,string,commands
def Join (JoinType, LeftDS, RightDS, DelimeterL, DelimeterR):
	# -============== INNER JOIN PART ==============-
	for i in xrange(len(LeftDS)):
		if i == 0: # first element is null
			continue
		for j in xrange(len(RightDS)):
			if j == 0: # first element is null
				continue
			print DelimeterL.join(LeftDS[i]) + DelimeterL + DelimeterR.join(RightDS[j])
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
	if JoinType !="Full" or JoinType !="Left" or JoinType !="Right" or JoinType !="Inner":
		print "Unknown join method, use Inner"
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
		try:
			line = line.strip()
			split_txt = line.split(IntermKeyDelim) #extract key and value (by default \t is separator)
			key = split_txt[0]	#extract key
			if key != PrevKey:
				PrevKey = key
				if iter == 1:
					if len(LeftDS) == 1 and (JoinType=="Full" or JoinType=="Right"):
						LeftDS.append(LeftZeroDS)
						DelimeterL = '' 
					if len(RightDS) == 1 and (JoinType=="Full" or JoinType=="Left"):
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
		except:
			print "Error:cannot parse row"		
	Join(JoinType, LeftDS, RightDS, DelimeterL, DelimeterR) #Join DSs s - last key
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