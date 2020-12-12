There are two separate parsers in this section of the repo. One to look for US States another for New York City boroughs.

To the run the scripts, please do the following:

1. Download the required directories and place them in the current working directory. To run the nltk library you must import a copy of the distribution to spark-submit, and that folder/zip file/etc must be in the current working directory to be found)

2. Load the relevant Python version and PySpark modules

   <pre><code>
   module load python/gnu/3.6.5
   module load spark/2.4.0
   </code></pre>

3. To generate the results for US states, run the following spark command

   <pre><code>
   time spark-submit \
   --py-files ah4896_modules.zip \
   --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
   check_states.py [CSV File Path]
   </code></pre> 

4. To generate the results for NYC boroughs, run the following spark command 

    <pre><code>
    time spark-submit \
    --py-files ah4896_modules.zip \
    --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
    check_boros.py [CSV File Path]
    </code></pre>  

5. Each column that is parsed from the input file will generate its own output file, with a name indicating the original file name, the column number and whether it was checking for states or boroughs. For example, 

<b>Note:</b> Not all columns are parsed. Only columns where the parser detects a valid value in the first 100 rows are screened
