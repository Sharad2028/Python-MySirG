from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.conf import SparkConf
import ETL.extract as e
import ETL.load as l
import ETL.Transform as t

import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("sharad").master("local[1]").getOrCreate()
path="C:\\Users\\shara\\OneDrive\\Desktop\\spark\\departments.csv"
configpath="C:\\Users\\shara\\OneDrive\\Desktop\\spark\\oracle_config.json"

def main():

    #Extract data
    oracleDF= e.extract_from_oracle(spark,configpath)

    csvDF= e.extract_from_csv(spark, path)

    #Transform Data
    transformedDF= t.transform_data(oracleDF,csvDF)

    #Load Data

    l.load_data(transformedDF, configpath)
    spark.stop()

if __name__=="__main__":
    main()

