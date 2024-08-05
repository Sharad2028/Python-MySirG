from pyspark.sql import SparkSession
import json

def load_config(config_path:str):
    with open(config_path,'r') as config_file:
        return json.load(config_file)

spark = (SparkSession.builder.
             config("spark.driver.extraClassPath", r"C:\Users\shara\PycharmProjects\pysparkpipline\lib\ojdbc8.jar").
             config("spark.executor.extraClassPath", r"C:\Users\shara\PycharmProjects\pysparkpipline\lib\ojdbc8.jar").appName("etl_pipline").master("local[*]").getOrCreate())
def extract_from_oracle(spark:SparkSession, config_path:str ):
    config=load_config(config_path)
    oracle_config= config['connection_properties']
    jdbc_url=oracle_config['url']
    connection_properties = {"user": oracle_config['user'],
                             "password": oracle_config['password'],
                             "driver": "oracle.jdbc.OracleDriver"
                             }
    df=spark.read.jdbc(url=jdbc_url,table='Employees', properties=connection_properties)
    return df

def extract_from_csv(spark:SparkSession, path:str):
    df=spark.read.format("csv").option("inferSchema", True).option("header", True).load(path)
    return df