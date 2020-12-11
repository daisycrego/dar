# module load python/gnu/3.4.4 && module load spark/2.2.0
# time spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python --py-files /home/dbc291/phonenumbers-8.12.13/dist/phonenumbers-8.12.13-py2.6.egg phone-number-v1.py /user/dbc291/data/311_Service_Requests_from_2010_to_Present.csv

from __future__ import print_function
import sys
import re
import phonenumbers
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


def checkValid(x):
    output = isValidNumber(x)
    if output[1] in (1, 2):
        return 1
    else:
        return 0 

def detectColNum(rows):
    column_breakdown = rows.map(lambda x: (x[0], x[1])) 
    column_numbers = column_breakdown.flatMap(lambda x: (map(lambda x: (x[1], x[0]), enumerate(x[0]))))
    #column_numbers.saveAsTextFile("column_numbers.out")  
    validated = column_numbers.map(lambda x: (x[1], checkValid((x[1], str(x[0])))))
    #validated.saveAsTextFile("validated.out")
    reduced = validated.reduceByKey(lambda a,b: a + b)
    detectedColumns = reduced.filter(lambda x: x[1] > 0)
    return detectedColumns.map(lambda x: x[0])
    

# validate a number assumed to be US but that may be missing the leading country code
def isValidNumber(x):
    """
    Returns a tuple with the validation status of x and the corrected value if the status is 2. 
    Statuses: 
    1) valid 
    2) incorrect version of a valid value - most common is missing the +1 (country code)
    3) invalid - semantic outlier (not a phone number)
    4) null - (999)-999-999, +1(888)888-888, 0000000000, etc.
    5) invalid - in phone number format, but not a valid phone number (e.g. 012-000-0000) because no us area codes begin with 0
    """
    try:
        phone_number = str(x[1])
        original_number = phone_number
        if phone_number is None:
            phone_number = ""
        row = x[0]
        # Strip out all the non-alphabetical chars and whitespace and get the str len. 
        phone_number = re.sub(r"[^a-zA-Z0-9+().-]", "", phone_number)
        phone_number = re.sub(r'\s', "", phone_number)
        str_len = len(phone_number)
        alpha = re.compile(r'[a-zA-Z]')
        if alpha.match(phone_number):
            return (row, 5, original_number) # this is a semantic outlier, it has alphabetic chars
        if str_len < 5 or str_len > 15:
            return (row, 5, original_number) # this is a semantic outlier, not possibly a phone number because of the length issues
        isValid = phonenumbers.is_valid_number(phonenumbers.parse(phone_number))
        isNull = checkForNull(phone_number)
        if not isValid and not isNull:
            if phone_number[0] == "1":
                phone_number = '{}{}'.format('+', phone_number)
                isValid = phonenumbers.is_valid_number(phonenumbers.parse(phone_number))
                return (row, 1, phone_number) if isValid else (row, 3, original_number)
            elif phone_number[0] == "+":
                return (row, 3, original_number) # already has a leading +
            else: 
                phone_number = "{}{}".format("+1", phone_number)
                isValid = phonenumbers.is_valid_number(phonenumbers.parse(phone_number))
                return (row, 1, phone_number) if isValid else (row, 4, original_number) if isNull else (row, 3, original_number)
        else:
            return (row, 1, phone_number) if isValid else (row, 4, original_number) if isNull else (row, 3, original_number)
    except phonenumbers.phonenumberutil.NumberParseException:
        if phone_number[0] == "1":
            phone_number = '{}{}'.format('+', phone_number)
            return isValidNumber((row, phone_number))
        elif phone_number[0] != "1" and phone_number[0] != "+": 
            phone_number = '{}{}'.format('+1', phone_number)
            return isValidNumber((row, phone_number))
        return (row, 3, original_number) # these can be considered syntactic outliers, they are not the correct format for this datatype


if __name__ == "__main__": 
    if len(sys.argv) != 2:
        print("Usage fp.py <data.csv>") # eg fp.py hw2/LicensesShort.csv
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
        formatted_results.saveAsTextFile('{}col_{}_parsed-phone-num_v1.out'.format(sys.argv[1], col))                                         
        