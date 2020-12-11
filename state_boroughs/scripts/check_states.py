from constants import *
from common_lang_functions import *
import sys
from csv import reader
from pyspark import SparkContext
from pyspark.sql import SparkSession
import string

spark = SparkSession \
	.builder \
	.appName("Python Spark SQL basic example") \
	.config("spark.some.config.option", "some-value") \
	.getOrCreate()

def checkAcronym(s):

    s = s.translate(str.maketrans('', '', string.punctuation))

    return s.upper() in STATE_ACRONYMS

def checkAbbreviation(s):

    l = [x.translate(str.maketrans('', '', string.punctuation)).lower() for x in STATE_ABBREVS]

    return s.translate(str.maketrans('','',string.punctuation)).lower() in l

def checkCapitalization(s):

    state = list(s.split(" "))

    if len(state) == 1:
        name = state[0]
        return name[0].upper() + name[1:].lower() in STATE_NAMES
    else:
        if "" not in state:
            capped_states = [x[0].upper()+x[1:].lower() for x in state]
            return " ".join(capped_states) in STATE_NAMES

def isValidState(s):

    row = s[0]
    state = s[1]

    try:
        if isNull(state):
            return (row,4,None)
        if state in STATE_NAMES:
            return (row,1,state)
        elif checkSpacing(state,STATE_NAMES) or checkAcronym(state) checkAbbreviation(state) or checkCapitalization(state):
            return (row,2,state)
        elif checkAbbreviation(state):
            return (row,2,state)
        else:
            return (row,3,state)

    except TypeError:
        return (row,3,state)

def checkValidCol(x):

    output = isValidState(x)
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
        validation = indexed_states.map(lambda x: (col, isValidState((x[1], x[0]))))

        formatted_results = validation.sortByKey()
        formatted_results.saveAsTextFile(sys.argv[1] + f"""col_{col}_parsed-state.out""")

    sc.stop()
