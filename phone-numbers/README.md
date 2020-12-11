## Phone Number Parser

- Welcome to the phone number parser. 2 versions of the parser are included, v0 and v1. v0 uses only string manipulation, regex, and other native python/pyspark functions. v1 adds the [phonenumbers](https://pypi.org/project/phonenumbers/) library. 
- To run the scripts, please follow the commands below. 

1. Make sure to download all of the required directories and place them in the current working directory (to run the phonenumbers library you must import a copy of the `phonenumbers` distribution to `spark-submit`, and that folder/zip file/etc must be in the current working directory to be found). 
2. Make sure to load the correct python and pyspark modules:
```bash
module load python/gnu/3.4.4 && module load spark/2.2.0
```
3. Run either the v0 or v1 script against some data: 
```bash
# to submit a v0 job
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python phone-number-v0.py /path/to/input/csv

# to submit a v1 job
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python --py-files phonenumbers-8.12.13/dist/phonenumbers-8.12.13-py2.6.egg phone-number-v1.py /path/to/input/csv
```
4. Each column parsed from the input file will generate its own output file, with a name like the following containing the original file name and the column name: `DOB_Permit_Issuance.csvcol_24_parsed-phone-num_v0.txt`. Not all columns will be parsed, only those that are seen by the parser to contain some phone numbers in the first 100 rows. 