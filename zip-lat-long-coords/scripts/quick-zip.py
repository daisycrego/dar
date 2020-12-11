# module load python/gnu/3.4.4 && module load spark/2.2.0
# spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python quick-zip.py input.csv
# note: for running the above spark submit command, please make sure all files and dependencies are located in accordance with the README.md

from __future__ import print_function
import sys
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import re

spark = SparkSession \
        .builder \
        .appName("QA-quickzip") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

#Status:
#1 - Valid (32117, 93117-4349, etc.)
#3 - Invalid - semantic outliers (doesn't look like a zip code)
#4 - Null 

#Note the status ranges for quick-zip  are singificantly impinged in the interest of time efficieny. This does result in a decrease in accuracy.
#For a more accurat zip code validation, please look at zip-validate.py

#Returns: boolean, true if input is numeric, false otherwise
def is_numeric(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

#Returns: a tuple with the row associated with the input -- this acts as a key to reduce by, 
# and a list of the column name of the input, the classification code (1 thru 5), and the input itself.
def formatCheckZip(x, col):
        inzip = x[0]
        row = x[1]
        inzip = str(inzip).strip()
        if "n" in inzip or "N" in inzip or inzip=="" or inzip == 0:
            return (row, [col, 4, inzip]) # input is null 
        elif len(''.join(filter(str.isdigit, inzip))) != 5 and len(''.join(filter(str.isdigit, inzip))) != 9:
            return  (row, [col, 3, inzip]) #input has the wrong number of digits, so it must be #3: invalid, not semantically corect
        elif len(inzip) == 5 and is_numeric(inzip):
            return (row, [col, 1, inzip]) #input is a valid verified zip code
        elif len(inzip) == 10 and  "-" in inzip and is_numeric(inzip[0:5] + inzip[6:10]):
            return (row, [col, 1, inzip]) # input is a valid verified zip code
        else: 
            return (row, [col, 3, inzip]) # 9 digits total, wrong format, not valid zipcode

#Returns a list of Column Names that have at least 5 valid (code: 1), or valid but badly formatted (code: 2) zipcodes as entries
def detectCols():
        zipList = []
        for i in header:
                zipCounter = 0
                subset = lines.take(100)
                for j in range(100):
                        if subset[j][i] != None:
                                if formatCheckZip((subset[j][i], 1),"col")[1][1] == 1 or formatCheckZip((subset[j][i], 1),"col")[1][1] == 2:
                                        zipCounter += 1
                if zipCounter >= 5:
                        zipList.append(i)               
        return zipList


if __name__ == "__main__":
  sc = spark.sparkContext
  
  lines = spark.read.csv(sys.argv[1], header="true", inferSchema="true")
  header = lines.schema.names 
  lines = lines.rdd #make an rdd out of the input csv

  detected_cols = detectCols()

  if len(detected_cols) == 0:
    print("No Columns Detected to be Zipcodes")
    exit(-1) #if there are no zipcode columns, exit

  for i in range(len(detected_cols)): #for each column detected as having zip codes, zip all it's entries with their row number, classify their QA status, sort by their key, and return them
    output= lines.map(lambda line: line[detected_cols[i]])
    indexed = output.zipWithIndex()
    validation = indexed.map(lambda x: (formatCheckZip((x[0], x[1]), detected_cols[i])))
    # validationZip.collect()
    results = validation.groupByKey().map(lambda x : (x[0], sorted(list(x[1]))))
    formatted_results = results.sortByKey()
    formatted_results.saveAsTextFile(sys.argv[1].split(".")[0]+"quickzip"+ str(i) + 'parsed.out') #look for outputs that begin with the name of the input file and have "quickzip" in their names. There may be more than 1.

  sc.stop
