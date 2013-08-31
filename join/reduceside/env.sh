#!/bin/sh
export LeftDSDelim="," #delimeter in left data set
export RightDSDelim="," #delimeter in left data set
export DSColCounts="3, 4" #column count in each dataset
export SelectIDX="0,1,2;0,1,2,3" #-1 means select all column from dataset
export JoinKeyIDX="0; 0" #it means join by first column by default
export JoinType="Inner"