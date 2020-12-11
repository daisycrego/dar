# Zip Code, Latitude, Longitude, and Coordinate Pairs Validation

Welcome to the location data parser. There are fours scripts, two of which identify columns with latitudes, longitudes, and a pairs of (lat, longs), and return Quality Assessment codes for every entry in each column. These two scripts use pyspark sql and regex, and require no additional files to be added to their spark submit commands.

The two other scripts identify columns of zip codes and return a QA code for each zip code. The first is quick-zip.py, which has a short run-time and is there good for very large datasets, but tends to be less accurate. T

The second is zip-validate.py which takes around 15-20 minutes to proces 5,000 - 10,000 rows, but does so with very high accuracy.  Zip-validate.py relies on the package zipcodes.py to determine whether a given input is a valid, registered US zip code. To use zip-validate, please follow the directions below carefully.

To run *any* of the scripts:
1. Make sure to load the correct python and pyspark modules:
```bash 
module load python/gnu/3.4.4 && module load spark/2.2.0
```
2. make sure your input.csv is loaded on the hfs
```bash
hfs -put input.csv
```
3. make sure you are in the same directory as the .py script, or that you include the file path to the .py script in your spark command
```bash
/projects/any-validate.py
```

To run the latitude/longitude validation:
```bash
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python latlong-validate.py input.csv
```
To run the single pair of coordinates validation:
```bash
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python coords-validate.py input.csv
```
To run the quick zipcode validation:
```bash
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python quick-zip.py input.csv
```

To run the zip-validate validation:
1. download the zipcodes.zip file to whichever directory you will send the spark submit command from
2. download the zips.json file to whichever directory you will send the submit command from
3. run:
```bash 
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python --py-files /home/user1234/zips.json,/home/user1234/zipcodes-1.1.2/zipcodes.zip  zip-validate.py input.csv
```

Each column parsed from the input file will generate its own output file, with a name like the following containing the original file name, the data type and an index: ```DOB_Job_Application_Filingslat0parsed.out```. Not all columns will be validated, only those that are seen by the parser to contain at least 5 instances of the desired data type in the first 100 rows.