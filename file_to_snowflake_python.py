# Databricks notebook source
from pyspark.sql import SQLContext
from pyspark import SparkConnf, SparkContex

class StageData:
    def __init__(self):
        self.table_name = dbutils.widgets.get("staging_table")
        self.data_file = dbutils.widgets.get("data_file")
        self.config_file = dbutils.widgets.get("config_file")
        self.v_delimiter = dbutils.widgets.get("p_delimiter")
        self.SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
        self.connect_configurator = configparser.ConfigParser()
        self.config_mode = os.getenv('CONFIG_MODE')
        self.pkb = dbutils.widgets.get("staging_table")
    
    def run(self):
        self.connect_configurator.read(self.config_file)
        sfOptions = {
            "sfURL" :,
            "sfUser" :,
            "pem_private_key" :,
            "sfDatabase" :,
            "sfRole" :,
            "sfWarehouse" :,
        }
        
        df = spark.read.options(inferSchema='True', header='True', delimiter=self.v_delimiter).csv(self.data_file)
        df.write.format(self.SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptions) \
            .options("dbTable", self.table_name) \
            .mode("overwrite") \
            .save()

# COMMAND ----------

if __name__ == '__main__':
    obj = StageData
    obj.run()
