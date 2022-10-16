# Databricks notebook source
# DBTITLE 1,Snowpark connector - using UserName & Pass
import os
from snowflake.snowpark import Session

user_name = dbutils.secrets.get(scope="dbr-secret-scope", key="username")
passwrd = dbutils.secrets.get(scope="dbr-secret-scope", key="password")

#specify only account name
connection_parameters = {
    "account": "bypgtvv-tw48419",
    "user": user_name,
    "password": passwrd,
    "role": "accountadmin",
    "warehouse": "MY_WH",
    "database": "TEST_DB",
    "schema": "TEST_SCHEMA"
}

session = Session.builder.configs(connection_parameters).create()

session.table("sample_product_data").show(2)

# COMMAND ----------

# DBTITLE 1,Spark Connector - Using Key Pair Authentication & Key Pair Rotation
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import re
import os

pkb = dbutils.secrets.get(scope="dbr-secret-scope", key="snowflake-spark-pkb")

# with open("/dbfs/secrets/pr_rsa_key.p8", "rb") as key_file:
#     p_key = serialization.load_pem_private_key(
#     key_file.read(),
#     password = passwrd.encode(),
#     backend = default_backend()
#     )

# pkb = p_key.private_bytes(
#   encoding = serialization.Encoding.PEM,
#   format = serialization.PrivateFormat.PKCS8,
#   encryption_algorithm = serialization.NoEncryption()
#   )

# pkb = pkb.decode("UTF-8")
# pkb = re.sub("-*(BEGIN|END) PRIVATE KEY-*\n","",pkb).replace("\n","")
# print(pkb)

sfOptions = {
  "sfURL" : "bypgtvv-tw48419.snowflakecomputing.com",
  "sfUser" : user_name,
  "pem_private_key" : pkb,
  "sfDatabase" : "TEST_DB",
  "sfSchema" : "TEST_SCHEMA",
  "sfWarehouse" : "MY_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option("dbTable", "sample_product_data") \
    .load()
#.option("autopushdown", "off") \

df.show(10)

# COMMAND ----------

# DBTITLE 1,Python Connector - Using Username & Password
import snowflake.connector

# Gets the version
ctx = snowflake.connector.connect(
    user=user_name,
    password=passwrd,
    account='bypgtvv-tw48419'
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()

# COMMAND ----------

# DBTITLE 1,(RSA File) Python Connector - Using Key Pair Authentication & Key Pair Rotation
import snowflake.connector
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

##1.way
with open("/dbfs/secrets/pr_rsa_key.p8", "rb") as key:
    p_key= serialization.load_pem_private_key(
        key.read(),
        password=passwrd.encode(),
        backend=default_backend()
    )

##2.way
#pkey, ln_key = codecs.escape_decode(dbutils.secrets.getBytes(scope="dbr-secret-scope", key="pr_rsa_key"))

##3.way
#pkey = dbutils.secrets.getBytes(scope="dbr-secret-scope", key="pr_rsa_key")

# p_key= serialization.load_pem_private_key(
#         pkey,
#         password=passwrd.encode(),
#         backend=default_backend()
#     )
    
pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

ctx = snowflake.connector.connect(
    user=user_name,
    account='bypgtvv-tw48419',
    private_key=pkb,
    warehouse="MY_WH",
    database="TEST_DB",
    schema="TEST_SCHEMA"
    )

cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()

# COMMAND ----------

# DBTITLE 1,(RSA string) Python Connector - Using Key Pair Authentication & Key Pair Rotation
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import os,hmac,json
import unicodedata

rsacert = unicodedata.normalize('NFKD', "\n-----BEGIN RSA PRIVATE KEY-----\n" + dbutils.secrets.get(scope="dbr-secret-scope", key="snowflake-spark-pkb") + "\n-----END RSA PRIVATE KEY-----\n").encode()

pkey = serialization.load_pem_private_key(
    rsacert,
    password = None,
    backend = None
    )

pkb = p_key.private_bytes(
  encoding = serialization.Encoding.DER,
  format = serialization.PrivateFormat.PKCS8,
  encryption_algorithm = serialization.NoEncryption()
  )

sfc = sf.connect(
    user=user_name,
    account='bypgtvv-tw48419',
    private_key=pkb,
    warehouse="MY_WH",
    database="TEST_DB",
    schema="TEST_SCHEMA",
    role="accountadmin"
)

cs = sfc.cursor()

try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()

# COMMAND ----------


