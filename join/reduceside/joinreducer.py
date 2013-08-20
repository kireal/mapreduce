#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Reduce step
#
# Kireal MapReduce Join Algorithm implementation
#	Simple python script to join to data set (files)
#	Reduce side implementation (map by key, tagging and loop output in reduce step)
#
# 20.08.2013

import sys,os,getopt,optparse,string,commands

def Join (JoinType, CitiesDS, CountriesDS):
	if len(JoinType) == 0:
		JoinType = "Inner"
	# -============== INNER JOIN PART ==============-
	if JoinType == "Inner":	#Inner
		for i in xrange(len(CitiesDS)):
			if i == 0: # first elenent is null
				continue
			for j in xrange(len(CountriesDS)):
				if j == 0: # first elenent is null
					continue
				print '%s; %s; %s; %s' %(CitiesDS[i][0], CitiesDS[i][2], CountriesDS[j][0], CitiesDS[i][1]) #City CountryCode CountryName Population
	# -============== LEFT OUTER JOIN PART ==============-
	elif JoinType == "Left": #Left outer join
		if len(CountriesDS)>1:
			for i in xrange(len(CitiesDS)):
				if i == 0: # first elenent is null
					continue
				for j in xrange(len(CountriesDS)):
					if j == 0: # first elenent is null
						continue
					print '%s; %s; %s; %s' %(CitiesDS[i][0], CitiesDS[i][2], CountriesDS[j][0], CitiesDS[i][1]) #City CountryCode CountryName Population
		else: #CountriesDS is empty, output only left DS
			for i in xrange(len(CitiesDS)):
				if i == 0: # first elenent is null
					continue
				print '%s; %s; %s; %s' %(CitiesDS[i][0], CitiesDS[i][2], "", CitiesDS[i][1]) #City CountryCode CountryName Population	
	# -============== RIGHT OUTER JOIN PART ==============-	
	elif JoinType == "Right": #Rigth outer join
		if len(CitiesDS)>1:
			for i in xrange(len(CitiesDS)):
				if i == 0: # first element is null
					continue
				for j in xrange(len(CountriesDS)):
					if j == 0: # first element is null
						continue
					print '%s; %s; %s; %s' %(CitiesDS[i][0], CitiesDS[i][2], CountriesDS[j][0], CitiesDS[i][1]) #City CountryCode CountryName Population
		else:	#CitiesDS is empty, output only right DS
			for i in xrange(len(CountriesDS)):
				if i == 0: # first element is null
					continue
				print '%s; %s; %s; %s' %("", "", CountriesDS[i][0], "") #City CountryCode CountryName Population
	# -============== FULL OUTER JOIN PART ==============-			
	elif JoinType == "Full": #Full outer join
		if len(CitiesDS)>1 and len(CountriesDS)>1:
			for i in xrange(len(CitiesDS)):
				if i == 0: # first element is null
					continue
				for j in xrange(len(CountriesDS)):
					if j == 0: # first element is null
						continue
					print '%s; %s; %s; %s' %(CitiesDS[i][0], CitiesDS[i][2], CountriesDS[j][0], CitiesDS[i][1]) #City CountryCode CountryName Population
		elif len(CountriesDS)==1 and len(CitiesDS)>1:	#CitiesDS is empty, output only right DS
			for i in xrange(len(CitiesDS)):
				if i == 0: # first element is null
					continue
				print '%s; %s; %s; %s' %(CitiesDS[i][0], CitiesDS[i][2], "", CitiesDS[i][1]) #City CountryCode CountryName Population
		elif len(CitiesDS)==1 and len(CountriesDS)>1:	#CountriesDS is empty, output only left DS
			for i in xrange(len(CountriesDS)):
				if i == 0: # first element is null
					continue
				print '%s; %s; %s; %s' %("", "", CountriesDS[i][0], "") #City CountryCode CountryName Population
	else:
		raise Exception("Unknown JoinType: " + JoinType)

def ReadStream (JoinType, CitiesDS, CountriesDS):
	CitiesDS = [[]]
	CountriesDS = [[]]
	PrevKey = []
	iter = 0
	for line in sys.stdin:
	#	try:
			line = line.strip()
			split_txt = line.split("\t") #extract key and value (by default \t is separator)
			key = split_txt[0]	#extract key
			if iter == 0: # optimeze this !
				PrevKey = key
				iter = 1
			if key != PrevKey:
				PrevKey = key
				Join(JoinType,CitiesDS, CountriesDS) #Join DSs
				CitiesDS = [[]] #Init DS
				CountriesDS = [[]] #Init DS
			value = split_txt[1].split(",") #extract value
			if value[0]=="CT":
				CitiesDS.append(value[1:])
			elif value[0]=="CN":
				CountriesDS.append(value[1:])
	Join(JoinType,CitiesDS, CountriesDS) #Join DSs - last key
	#	except:
	#		print "*** Error join in two rows" + str(sys.argv[1:])
CitiesDS = [[]]
CountriesDS = [[]]
JoinType = []
try:
	JoinType = os.environ['JoinType']
except:
	pass
ReadStream(JoinType, CitiesDS, CountriesDS) 