# module load python/gnu/3.4.4 && module load spark/2.2.0
# spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python latlong-validate.py input.csv
# note: for running the above spark submit command, please make sure all files and dependencies are located in accordance with the README.md

from __future__ import print_function
import sys
from csv import reader
import zipcodes
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from math import isclose
import re
import datetime

spark = SparkSession \
        .builder \
        .appName("QArev1") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

#Status:
#1 - Valid - eg: -73.08432, 99.45455, etc,
#2 - Invalidly-formatted, valid value - eg: -73.08432North, 99.45455W, etc
#3 - Invalid - semantic outliers (doesn't look like a latitude or longitude)
#4 - Null 
#5 - Invalid - semantically correct (is a number), but is not in the valid range

#Returns: boolean, true if input is numeric, false otherwise
def is_numeric(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

#Returns: a tuple with the row associated with the input -- this acts as a key to reduce by, 
# and a list of the column name of the input and lat/long type, the classification code (1 thru 5), and the input itself.
def latLongCheck(x, maxim, col):
        coord = x[0]
        row = x[1]
        coord = str(coord).strip()
        if maxim == "lat": #latitdues are between [-90.0, 90.0]
                maximv = 90
        else:
                maximv = 180 #longitdues are between [-180.0, 180.0]

        if coord == "" or "none" in coord.lower() or "null" in coord.lower() or "n/a" in coord.lower():
                return (row, [maxim, col, 4, coord]) #input is null
        else:
                if is_numeric(coord) and len(coord)>5:
                                coord = float(coord)
                                if -1*maximv <= coord and coord <= maximv:
                                        return (row, [maxim, col, 1, coord]) #input is a number in the appropriate range, it's valid
                                else:
                                        return (row, [maxim, col, 5, coord]) #input is a number but is not in the appropriate range
                elif is_numeric(coord.replace("N", "").replace("W", "")) and len(coord) > 5:
                        if -1*maximv <= float(coord.replace("N", "").replace("W", "")) and float(coord.replace("N", "").replace("W", ""))  <= maximv:
                                return (row, [maxim, col, 2, coord]) #invalid format (input had associated direction N for north or W for West), but once those are stripped out, it is in the correct range
                        else:
                                return (row, [maxim, col, 3, coord]) #input has digits and N and W, but those digits are invald
                else:
                        return (row, [maxim, col, 3, coord]) #input is not a number, it's invalid



#Returns a list of Column Names that have at least 5 valid (code: 1), or valid but badly formatted (code: 2) lat/long as entries
def detectCols():
        latlongList = []
        for i in header:
                latLongCounter = 0
                subset = lines.take(100)
                for j in range(100):
                        if subset[j][i] != None:
                                if ("lat" in i.lower() or "long" in i.lower())and (latLongCheck((subset[j][i], 1), "long" ,"col")[1][2] == 1 or latLongCheck((subset[j][i], 1),"long", "col")[1][2] == 2):
                                        latLongCounter += 1
                if latLongCounter >= 5:
                        latlongList.append(i)               
        return latlongList


if __name__ == "__main__":
  sc = spark.sparkContext
  
  lines = spark.read.csv(sys.argv[1], header="true", inferSchema="true")
  header = lines.schema.names 
  lines = lines.rdd #make an rdd out of the input csv

  detected_cols = detectCols()

  if len(detected_cols) == 0:
    print("No Columns Detected to be Latitudes or Longitudes")
    exit(-1) #if there are no lat/lon columns, exit

  for i in range(len(detected_cols)): #for each column detected as having lat/long, zip all it's entries with their row number, classify their QA status, sort by their key, and return them
    if "lat" in detected_cols[i].lower():
        maxim = "lat"
    else:
        maxim = "long"
    output= lines.map(lambda line: line[detected_cols[i]])
    indexed = output.zipWithIndex()
    validation = indexed.map(lambda x: (latLongCheck((x[0], x[1]), maxim, detected_cols[i])))
    # validationZip.collect()
    results = validation.groupByKey().map(lambda x : (x[0], sorted(list(x[1]))))
    formatted_results = results.sortByKey()
    formatted_results.saveAsTextFile(sys.argv[1].split(".")[0]+maxim+ str(i) + 'parsed.out') #look for outputs that begin with the name of the input file and have "lat"  or "long" in their names. There may be more than 1.

  sc.stop
