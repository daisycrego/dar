from constants import *
from common_lang_functions import *
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession \
	.builder \
	.appName("Python Spark SQL basic example") \
	.config("spark.some.config.option", "some-value") \
	.getOrCreate()

def checkCapitalization(b):

    # Check if the name is in all caps
    if b not in BORO_NAMES:
        if b[0].upper() + b[1:].lower() in BORO_NAMES:
            return True
    else:
        return False

def checkAbbreviation(b):

    # Check if the borough is a valid abbreviation
    return b in BORO_ABBREVS

def isValidBoro(b):

    row = b[0]
    boro = b[1]

    try:
        if isNull(boro):
            return (row,4,None)
        elif boro in BORO_NAMES:
            return (row,1,boro)
        elif checkSpacing(boro,BORO_NAMES) or checkAbbreviation(boro) or checkCapitalization(boro):
            return (row,2,boro)
        else:
            return (row,3,boro)
    except TypeError:
        return (row,3,boro)

def checkValidCol(x):

    output = isValidBoro(x)
    if output[1] in (1, 2):
        return 1
    else:
        return 0

def detectColNum(rows):
    column_breakdown = rows.map(lambda x: (x[0], x[1]))
    column_numbers = column_breakdown.flatMap(lambda x: (map(lambda x: (x[1], x[0]), enumerate(x[0]))))
    validated = column_numbers.map(lambda x: (x[1], checkValidCol((x[1], x[0]))))
    reduced = validated.reduceByKey(lambda a,b: a + b)
    detectedColumns = reduced.filter(lambda x: x[1] > 0)
    return detectedColumns.map(lambda x: x[0])

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage fp.py <data.csv>")
        exit(-1)

    sc = spark.sparkContext
    lines = spark.read.csv(sys.argv[1], header="true", inferSchema="true")
    lines = lines.rdd
    subset = lines.zipWithIndex().filter(lambda x: x[1] < 100)
    detected_cols = detectColNum(subset).collect()

    for col in detected_cols:
        output = lines.map(lambda line: line[col] if len(line) > (col) else "")
        indexed_states = output.zipWithIndex().filter(lambda x: x[1] > 0)
        validation = indexed_states.map(lambda x: (col, isValidBoro((x[1], x[0]))))

        formatted_results = validation.sortByKey()
        formatted_results.saveAsTextFile(sys.argv[1] + f"""col_{col}_parsed-boro.out""")

    sc.stop()
