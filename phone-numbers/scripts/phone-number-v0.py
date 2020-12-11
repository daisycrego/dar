# module load python/gnu/3.4.4 && module load spark/2.2.0
# spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python phone-number-v0.py /user/hc2660/hw2data/Licenses.csv

from __future__ import print_function
import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
	.builder \
	.appName("Python Spark SQL basic example") \
	.config("spark.some.config.option", "some-value") \
	.getOrCreate()

#Status:
#1 - Valid (+12013347135, 2013347135, etc.)
#2 - Invalidly-formatted, valid value
#3 - Invalid - semantic outliers (doesn't look like a phone number)
#4 - Null - 999-999-9999, etc
#5 - Invalid - semantically correct, but not a valid phone number (leading 0 in the area code, area code doesn't exist, etc)

def checkForNull(x):
    # A null phone number (aside from an empty string) has all consecutive digits, e.g. 999-999-9999
    # Note that it still must have 10 digits, although 12 is acceptable if it's +19999999999
    # Note --> we are assuming the input string was already stripped of non-alphabetic chars and spaces
    str_len = len(x) 
    if not str_len:
        return True
    if str_len >= 5 and str_len < 15: 
        # check for leading + or +1
        if "+" in x[:2]:
            if x[:2] == "+1":
                return checkForNull(x[2:])
            else:
                return checkForNull(x[1:])
        else: 
            return True if x.count(x[0]) == len(x) else False
    else: 
        return False 


def isValidNumber(x):
    """
    ^\+?[1-9]\d{4,14}$
        - No country codes start with 0
        - The shortest number is 5 digits, including the country code, e.g. +64010 (NZ)
    """
    phone_number = str(x[1])
    original_number = phone_number
    if phone_number is None:
        phone_number = ""
    row = x[0]
    # Strip out all the non-word (A-Za-z0-9) chars and whitespace and get the str len. 
    phone_number = re.sub(r'\W', "", phone_number)
    phone_number = re.sub(r'\s', "", phone_number)
    str_len = len(phone_number)
    pattern = r"\+(9[976]\d|8[987530]\d|6[987]\d|5[90]\d|42\d|3[875]\d|2[98654321]\d|9[8543210]|8[6421]|6[6543210]|5[87654321]|4[987654310]|3[9643210]|2[70]|7|1)\d{1,14}$"
    # Regex for int'l phone numbers standard E.164 found here: http://en.wikipedia.org/wiki/E.164
    # Then modified based on these comments: https://stackoverflow.com/questions/6478875/regular-expression-matching-e-164-formatted-phone-numbers
    regex = re.compile(pattern)
    alpha = re.compile(r'[a-zA-Z]')
    if alpha.match(phone_number):
        return (row, 5, original_number) # this is a semantic outlier, it has alphabetic chars
    if str_len < 5 or str_len > 15:
        return (row, 5, original_number) # this is a semantic outlier, not possibly a phone number because of the length issues
    # do an initial check on the number
    isValid = regex.match(phone_number) 
    isNull = checkForNull(phone_number) 
    # if the first check fails, try appending an international code to the value
    if not isValid and not isNull:
        if phone_number[0] == "1": 
            phone_number = '{}{}'.format('+', phone_number)
            isValid = regex.match(phone_number)
            return (row, 1, phone_number) if isValid else (row, 3, original_number)
        elif phone_number[0] == "+":
            return (row, 3, phone_number) # this already has a leading +, it should have the full int'l number and the prev check should be used 
        else:
            phone_number = '{}{}'.format('+1', phone_number)
            isValid = regex.match(phone_number)
            return (row, 1, phone_number) if isValid else (row, 3, original_number)         
    else:
        return (row, 1, phone_number) if isValid else (row, 4, original_number) if isNull else (row, 3, original_number) 


def checkValid(x):
    output = isValidNumber(x)
    if output[1] in (1, 2):
        return 1
    else:
        return 0 


def detectColNum(rows):
    column_breakdown = rows.map(lambda x: (x[0], x[1]))
    column_numbers = column_breakdown.flatMap(lambda x: (map(lambda x: (x[1], x[0]), enumerate(x[0]))))
    validated = column_numbers.map(lambda x: (x[1], checkValid((x[1], str(x[0])))))
    reduced = validated.reduceByKey(lambda a,b: a + b)
    detectedColumns = reduced.filter(lambda x: x[1] > 0)
    return detectedColumns.map(lambda x: x[0])


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage fp.py <data.csv>") # eg phone-number-v0.py hw2/LicensesShort.csv
        exit(-1)

    sc = spark.sparkContext
    lines = spark.read.csv(sys.argv[1], header="true", inferSchema="true")
    lines = lines.rdd
    subset = lines.zipWithIndex().filter(lambda x: x[1] < 100)
    detected_cols = detectColNum(subset).collect()
    
    if not len(detected_cols):
        print("No phone numbers detected")
        exit(-1)

    for col in detected_cols:
        output = lines.map(lambda line: line[col] if len(line) > (col) else "")
        indexed_numbers = output.zipWithIndex().filter(lambda x: x[1] > 0) 
        validation = indexed_numbers.map(lambda x: (col, isValidNumber((x[1], x[0]))))
        formatted_results = validation.sortByKey()
        formatted_results.saveAsTextFile('{}col_{}_parsed-phone-num_v0.out'.format(sys.argv[1], col)) 