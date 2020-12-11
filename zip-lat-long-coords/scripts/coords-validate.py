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
#1 - Valid (+12013347135, 2013347135, etc.)
#2 - Invalidly-formatted, valid value
#3 - Invalid - semantic outliers (doesn't look like a phone number)
#4 - Null - 999-999-9999, etc
#5 - Invalid - semantically correct, but not a valid phone number (leading 0 in the area code, area code doesn't exist, etc)

#Returns: boolean, true if input is numeric, false otherwise
def is_numeric(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

#Returns: a tuple with the row associated with the input -- this acts as a key to reduce by, 
# and a list of the column name of the input, the classification code (1 thru 5), and the input itself.
def singleCoord(x, col):
        incoord = x[0]
        row = x[1]
        incoord = str(incoord).strip() #strip leading/trailing whitespace 
        incoord = incoord.replace("[", "").replace("]", "").replace(")", "").replace("(", "") # strip brackets/parantheses (these are common in formatting coordinates)
        if (len(incoord.split(" ")) == 1) and ("n" in incoord or "N" in incoord or incoord=="" or incoord == 0):
                return (row, [col, 4, incoord])  # input is null
        elif "," in incoord and len(incoord.split(",")) == 2:
                return (row, [col, 1, latLongforSingle(incoord.split(",")[0], "lat"), latLongforSingle(incoord.split(",")[1], "long")]) #input is two strings split by a comma, as a whole it's valid, but the validity of the parts will be determine via latLongforSingle()
        elif " " in incoord and len(incoord.split(" ")) == 2:
                return (row, [col, 1, latLongforSingle(incoord.split(" ")[0], "lat"), latLongforSingle(incoord.split(" ")[1], "long")]) ##input is two strings split by a space, as a whole it's valid, but the validity of the parts will be determine via latLongforSingle()
        elif len(re.split(r'\D+', incoord)) == 4 or len(re.split(r'\D+', incoord)) == 5:
                return (row, [col, 2, incoord]) # input is two values split by some demarcation (punctuation, symbol, character -- eg "92.094576x19.23445"), it is an invalid format but may be a valid value
        else:
                return (row, [col, 3, incoord]) #input does not look like a pair of coordinates


#Returns: a list of the column name of the input and lat/long type, the classification code (1 thru 5), and the input itself.
def latLongforSingle(x, maxim):
    coord_sing = str(x).strip()
    if maxim == "lat":
        maximv = 90
    else:
        maximv = 180
    if coord_sing == "" or "none" in coord_sing.lower() or "null" in coord_sing.lower() or "n/a" in coord_sing.lower():
        return ([maxim, 4, coord_sing]) #if coordinate is null
    else:
        if is_numeric(coord_sing) and len(coord_sing) >5:
            coord_sing = float(coord_sing)
            if -1*maximv <= coord_sing and coord_sing <= maximv:
                    return ([maxim, 1, coord_sing]) #coordinate is a valid number
            else:
                    return ([maxim, 3, coord_sing]) #coordinate is an invalid number -- right now this is cat as invalid, semantically invalid, but it could be switch$
        elif is_numeric(coord_sing.replace("N", "").replace("W", "")) and len(coord_sing) > 5:
                if -1*maximv <= float(coord_sing.replace("N", "").replace("W", "")) and float(coord_sing.replace("N", "").replace("W", ""))  <= maximv:
                        return ([maxim, 2, coord_sing]) #invalid format (some character or symbol separator is weird), but once those are stripped out, in the correct range
                else:
                        return ([maxim, 3, coord_sing]) #invalid format and invalaid digits
        else:
                return ([maxim, 3, coord_sing])


#Returns a list of Column Names that have at least 5 valid (code: 1), or valid but badly formatted (code: 2) zipcodes as entries
def detectCols():
        coordList = []
        for i in header:
                coordCounter = 0
                subset = lines.take(100)
                for j in range(100):
                        if subset[j][i] != None:
                                if (singleCoord((subset[j][i], 1),"col")[1][1] == 1 or singleCoord((subset[j][i], 1),"col")[1][1] == 2) and (singleCoord((subset[j][i], 1),"col")[1][2][1] == 1 or singleCoord((subset[j][i], 1),"col")[1][2][1]==2):
                                        coordCounter += 1
                if coordCounter >= 5:
                        coordList.append(i)               
        return coordList


if __name__ == "__main__":
  sc = spark.sparkContext
  
  lines = spark.read.csv(sys.argv[1], header="true", inferSchema="true")
  header = lines.schema.names 
  lines = lines.rdd #make an rdd out of the input csv

  detected_cols = detectCols()

  if len(detected_cols) == 0:
    print("No Columns Detected to be Coordinates")
    exit(-1) #if there are no coordinate columns, exit

  for i in range(len(detected_cols)): #for each column detected as having coordinates, zip all it's entries with their row number, classify their QA status, sort by their key, and return them
    output= lines.map(lambda line: line[detected_cols[i]])
    indexed = output.zipWithIndex()
    validation = indexed.map(lambda x: (singleCoord((x[0], x[1]), detected_cols[i])))
    # validationZip.collect()
    results = validation.groupByKey().map(lambda x : (x[0], sorted(list(x[1]))))
    formatted_results = results.sortByKey()
    formatted_results.saveAsTextFile("coord"+ str(i) + 'parsed.out')
 

  sc.stop
