# module load python/gnu/3.4.4 && module load spark/2.2.0
# spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python --py-files /home/user1234/zips.json,/home/user1234/zipcodes-1.1.2/zipcodes.zip  zip-validate.py input.csv
# note: for running the above spark submit command, please make sure all files and dependencies are located in accordance with the README.md

from __future__ import print_function
import sys
from csv import reader
import zipcodes
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import re
import datetime

spark = SparkSession \
        .builder \
        .appName("QAzip") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

#Status:
#1 - Valid - 32117, 93117-4349, etc.
#2 - Invalidly-formatted, valid value  - eg 931174349
#3 - Invalid - semantic outliers, wrong length -- doesn't look like a zipcode
#4 - Null 
#5 - Invalid - semantically correct, but not a valid zipcode (as determined by the zipcodes package)

def is_numeric(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def formatCheckZip(x, col):
        inzip = x[0]
        row = x[1]
        inzip = str(inzip).strip()
        if "n" in inzip or "N" in inzip or inzip=="" or inzip == 0:
                return (row, [col, 4, inzip]) #null expression
        elif len(''.join(filter(str.isdigit, inzip))) != 5 and len(''.join(filter(str.isdigit, inzip))) != 9:
                return  (row, [col, 3, inzip]) #wrong number of digits, so must be invalid, not semantically corect
        elif len(inzip) == 5 and is_numeric(inzip):
                if zipcodes.is_real(inzip) == True:
                        return (row, [col, 1, inzip]) #this is valid
                else:
                        return (row, [col, 5, inzip]) #5 digits appearing in standard format, but not a true zipcode (in the US)
        elif len(inzip) == 10 and  "-" in inzip and is_numeric(inzip[0:5] + inzip[6:10]):
                if zipcodes.is_real(inzip[0:5]+"-"+inzip[6:10]):
                        inzip = inzip[0:5]+"-"+inzip[6:10]
                        return (row, [col, 1, inzip]) #valid
                else:
                        return (row, [col, 5, inzip])
        elif len(inzip) == 9 and  is_numeric(inzip):
                if zipcodes.is_real(inzip[0:5]+"-"+inzip[5:9]):
                        inzip = inzip[0:5]+"-"+inzip[5:9]
                        return (row, [col, 2, inzip]) #valid
                else:
                        return (row, [col, 3, inzip])
        else:
                if len(''.join(filter(str.isdigit, inzip)))==5:
                        inzip = ''.join(filter(str.isdigit, inzip))
                        if zipcodes.is_real(''.join(filter(str.isdigit, inzip))):
                                return (row, [col, 2, inzip]) #5 digits total, wrong format but valid zipcode
                        else:
                                return (row, [col, 3, inzip]) #5 digits total,  wrong format, not valid zipcodes
                else:
                        return (row, [col, 3, inzip]) # 9 digits total, wrong format, not valid zipcode

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
  lines = lines.rdd

  detected_cols = detectCols()

  if len(detected_cols) == 0:
    print("No Columns Detected to be Zipcodes")
    exit(-1)

  for i in range(len(detected_cols)): #zip detected cols
    output= lines.map(lambda line: line[detected_cols[i]])
    indexed = output.zipWithIndex()
    validation = indexed.map(lambda x: (formatCheckZip((x[0], x[1]), detected_cols[i])))
    # validationZip.collect()
    results = validation.groupByKey().map(lambda x : (x[0], sorted(list(x[1]))))
    formatted_results = results.sortByKey()
    formatted_results.saveAsTextFile(sys.argv[1].split(".")[0]+"zip"+ str(i) + 'parsed.out')  #look for outputs that begin with the name of the input file and have "zip" in their names. There may be more than 1.

  sc.stop()
