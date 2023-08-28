# Databricks notebook source
# import json
# from pyspark.sql.functions import col, when, expr, lit, current_date, current_timestamp
# from pyspark.sql.types import *
# from functools import reduce

# base_path = "/dbfs/test/project/system_mapping/"

# scourcefile = base_path + dbutils.widgets.get("sourceSchema")
# targetfile = base_path + dbutils.widgets.get("targetSchema")
# mappingfile = base_path + dbutils.widgets.get("mappingFile")

# def readJson(filename):
#     with open(filename+".json", "r") as sf:
#         return json.load(sf)

# # Load the mapping JSON file
# source_schema = readJson(scourcefile)
# mapping = readJson(mappingfile)

# schema = StructType(reduce(
#     lambda acc, x: acc + [StructField(x["name"], StringType(), True)],
#     source_schema["properties"],
#     []
# ))

# options = {
#     "header": source_schema['header'],
#     "delimiter": source_schema['field_delimiter']
# }

# source_df = spark.read.options(**options).schema(schema).csv("/test/project/system_mapping/input.csv")

# source_df.show()

# if source_schema['isFixedWidth']:
#     name_positions = {}
#     for prop in source_schema["properties"]:
#         name_positions[prop["name"]] = [int(p) for p in prop["position"].split(",")]
#     col_exprs = [expr(f"substring({source_df.columns[0]}, {pos[0]}, {pos[1]}) as {name}") for name, pos in name_positions.items()]

#     # Apply the expressions to the DataFrame
#     source_df = source_df.select(col_exprs)

# source_df.show()


# COMMAND ----------


# # Create a list of PySpark expressions to transform the columns
# exprs = []

# # extract mapping for each column
# for mp in mapping["mappings"]:
    
#     source_name = mp["source"]["name"]
#     source_type = mp["source"]["type"]
#     target_name = mp["target"]["name"]
#     target_type = mp["target"]["type"]
#     expression_flag =  True if "expression" in mp["target"] else False
    
#     # Check if expression exist for specific column
#     if expression_flag:
#         # Create a PySpark expression using the SQL expression
#         col_expr = expr(mp["target"]["expression"]).alias(target_name)
#     else:
#         # Cast the source column to the target data type
#         col_expr = col(source_name).cast(target_type).alias(target_name)
    
#     exprs.append(col_expr)
#     print(col_expr)

# # Apply the expressions to the source DataFrame to create the target DataFrame
# target_df = source_df.select(*exprs)
# print("\n")
# target_df.show()


# COMMAND ----------

import logging

class LogUtils:

    @staticmethod
    def logger():
        logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
        log = logging.getLogger(__name__)
        return log

# COMMAND ----------


import json
import requests

class Utils:

    @staticmethod
    def readJson(object):
        with open(object, "r") as sf:
            return json.load(sf)

    @staticmethod
    def convertToDictionary(object):
        if isinstance(object, str):
            return json.loads(object)
        else:
            if object is not None:
                dictionary = object.__dict__
            else:
                dictionary = None
        return dictionary

    @staticmethod
    def convertToJson(object):
        return json.dumps(object) 

    @staticmethod
    def postRequest(url, payload, headers, proxies=None):
        if proxies is None:
            response = requests.post(url, data=payload, headers=headers)
        else:
            response = requests.post(url, data=payload, headers=headers, proxies=proxies, verify=False)
        return response

    @staticmethod
    def getRequest(url, input_params, bearer_token=None):
        if bearer_token is not None:
            headers = {"Authorization": "Bearer {0}".format(bearer_token)}
            response = requests.get(url, params=input_params, headers=headers)
        else:
            response = requests.get(url, params=input_params)
        return response


# COMMAND ----------

class MappingServiceConstants:
    
    sfDefaultStagingSchema = "STAGING"
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    fileFormatQueryCSVArgs = {
        "field_delimiter": ",",
        "skip_header": "0",
        "comperssion": "AUTO",
        "record_delimiter": "\\n",
        "skip_blank_lines": "FALSE",
        "date_format": "AUTO",
        "time_format": "AUTO",
        "timestamp_format": "AUTO",
        "binary_format": "HEX",
        "escape": "NONE",
        "escape_unenclosed_field": "NONE",
        "trim_space": "FALSE",
        "field_optionally_enclosed_by": "NONE",
        "error_on_column_count_mismatch": "TRUE",
        "replace_invalid_characters": "FALSE",
        "empty_field_as_null": "TRUE",
        "skip_byte_order_mark": "TRUE",
        "encoding": "UTF8",
        "file_extension": ""
    }

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, expr, lit, current_date, current_timestamp
from pyspark.sql.types import *
from functools import reduce


class ReadFeedService:
    def __init__(self, base_path):
        self.base_path = base_path

    def run(self) -> DataFrame:
        scourcefile = self.base_path + dbutils.widgets.get("sourceSchema")
        datafile = dbutils.widgets.get("dataFile")

        # Load the Source Schema file
        source_schema = Utils.readJson(scourcefile)
        
        # Create schema object for source data file
        schema = StructType(reduce(
            lambda acc, x: acc + [StructField(x["name"], StringType(), True)], source_schema["properties"],[]
        ))

        options = {
            "header": source_schema['header'],
            "delimiter": source_schema['field_delimiter']
        }

        df = spark.read.options(**options).schema(schema).csv(datafile)

        return df

# COMMAND ----------

if __name__ == '__main__':
    base_path = "/dbfs/test/project/system_mapping/"
    obj = ReadFeedService(base_path)
    obj.run().show()

# COMMAND ----------

import json
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, expr, lit, current_date, current_timestamp
from pyspark.sql.types import *
from functools import reduce

class MappingService:
    def __init__(self, base_path):
        self.base_path = base_path

    def run(self, source_df) -> DataFrame:
        mappingfile = self.base_path + dbutils.widgets.get("mappingFile")

        # Load the Mapping JSON file
        mapping = Utils.readJson(mappingfile)
        
        exprs = []
        # extract mapping for each column
        for mp in mapping["mappings"]:
            
            source_name = mp["source"]["name"]
            source_type = mp["source"]["type"]
            target_name = mp["target"]["name"]
            target_type = mp["target"]["type"]
            expression_flag =  True if "expression" in mp["target"] else False
            
            # Check if expression exist for specific column
            if expression_flag:
                # Create a PySpark expression using the SQL expression
                col_expr = expr(mp["target"]["expression"]).alias(target_name)
            else:
                # Cast the source column to the target data type
                col_expr = col(source_name).cast(target_type).alias(target_name)
            
            exprs.append(col_expr)
            print(col_expr)

        # Apply the expressions to the source DataFrame to create the target DataFrame
        target_df = source_df.select(*exprs)
        return target_df

# COMMAND ----------

if __name__ == '__main__':
    base_path = "/dbfs/test/project/system_mapping/"
    obj = MappingService(base_path)
    obj.run(ReadFeedService(base_path).run()).show()

# COMMAND ----------


