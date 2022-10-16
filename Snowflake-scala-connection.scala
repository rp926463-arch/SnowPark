// Databricks notebook source
// DBTITLE 1,Snowpark - Connect to Snowflake using UserName & Pass
import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._

var user_name = dbutils.secrets.get(scope="dbr-secret-scope", key="username")
var passwrd = dbutils.secrets.get(scope="dbr-secret-scope", key="password")

//specify URL : https://account_identifier.snowflakecomputing.com
var sfOptions = Map(
      "url" -> "bypgtvv-tw48419.snowflakecomputing.com",
      "user" -> user_name,
      "password" -> passwrd,
      "role" -> "accountadmin",
      "warehouse" -> "MY_WH",
      "database" -> "TEST_DB",
      "schema" -> "TEST_SCHEMA"
    )
val session = Session.builder.configs(sfOptions).create
session.table("sample_product_data").show(2)

// COMMAND ----------

// DBTITLE 1,Snowpark - Connect to Snowflake using Unencrypted Private Key
// Create a new session, using the connection properties
// specified in a Map.

//openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out pr_rsa_key_une.p8 -nocrypt
//openssl rsa -in pr_rsa_key_une.p8 -pubout -out pb_rsa_key_une.pub
//alter user jsmith set rsa_public_key='MIIBIjANBgkqh...';

val session = Session.builder.configs(Map(
    "URL" -> "https://bypgtvv-tw48419.snowflakecomputing.com",
    "USER" -> user_name,
    "PRIVATE_KEY_FILE" -> "/dbfs/secrets/pr_rsa_key_une.p8",
    "ROLE" -> "accountadmin",
    "WAREHOUSE" -> "MY_WH",
    "DB" -> "TEST_DB",
    "SCHEMA" -> "TEST_SCHEMA"
)).create

session.table("sample_product_data").show(2)

// COMMAND ----------

// DBTITLE 1,Snowpark - Connect to Snowflake using encrypted Private Key
// Create a new session, using the connection properties
// specified in a Map.

//openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -inform PEM -out pr_rsa_key.p8
//openssl rsa -in pr_rsa_key.p8 -pubout -out pb_rsa_key.pub
//alter user jsmith set rsa_public_key_2='MIIBIjANBgkqh...';


val pkb = dbutils.secrets.get(scope="dbr-secret-scope", key="snowflake-spark-pkb")

val session = Session.builder.configs(Map(
    "URL" -> "https://bypgtvv-tw48419.snowflakecomputing.com",
    "USER" -> user_name,
    "PRIVATE_KEY" -> pkb,
    "PASSWORD" -> passwrd,
    "ROLE" -> "accountadmin",
    "WAREHOUSE" -> "MY_WH",
    "DB" -> "TEST_DB",
    "SCHEMA" -> "TEST_SCHEMA"
)).create

session.table("sample_product_data").show(2)

// COMMAND ----------

// DBTITLE 1,Spark Connector - Using Username & Password
import org.apache.spark.sql._
import net.snowflake.spark.snowflake.Utils
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
//
// Configure your Snowflake environment
//
var sfOptions = Map(
    "sfURL" -> "bypgtvv-tw48419.snowflakecomputing.com",
    "sfUser" -> user_name,
    "sfPassword" -> passwrd,
    "sfDatabase" -> "TEST_DB",
    "sfSchema" -> "TEST_SCHEMA",
    "sfWarehouse" -> "MY_WH"
    )

//
// Create a DataFrame from a Snowflake table
//
val df: DataFrame = sqlContext.read
    .format(SNOWFLAKE_SOURCE_NAME)
    .options(sfOptions)
    .option("dbtable", "MY_TABLE")
    .load()

//
// DataFrames can also be populated via a SQL query
//
val new_df: DataFrame = sqlContext.read
    .format(SNOWFLAKE_SOURCE_NAME)
    .options(sfOptions)
    .option("query", "select A, count(*) from MY_TABLE group by A")
    .load()

//
// Join, augment, aggregate, etc. the data in Spark and then use the
// Data Source API to write the data back to a table in Snowflake
//

df.write
    .format(SNOWFLAKE_SOURCE_NAME)
    .options(sfOptions)
    .option("dbtable", "t2")
    .mode(SaveMode.Overwrite)
    .save()

//Executing DDL/DML SQL Statements
//Use the runQuery() method of the Utils object to execute DDL/DML SQL statements, in addition to queries, for example
Utils.runQuery(sfOptions, "CREATE OR REPLACE TABLE MY_TABLE(A INTEGER)")


// COMMAND ----------

// DBTITLE 1,Spark Connector - Using Key Pair Authentication & Key Pair Rotation
import org.apache.spark.sql._
import net.snowflake.spark.snowflake.Utils
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
//
// Configure your Snowflake environment
//

var pkb = dbutils.secrets.get(scope="dbr-secret-scope", key="snowflake-spark-pkb")

var sfOptions = Map(
    "sfURL" -> "bypgtvv-tw48419.snowflakecomputing.com",
    "sfUser" -> user_name,
    "pem_private_key" -> pkb,
    "sfDatabase" -> "TEST_DB",
    "sfSchema" -> "TEST_SCHEMA",
    "sfWarehouse" -> "MY_WH"
    )

//
// Create a DataFrame from a Snowflake table
//
val df: DataFrame = sqlContext.read
    .format(SNOWFLAKE_SOURCE_NAME)
    .options(sfOptions)
    .option("dbtable", "MY_TABLE")
    .load()

df.show(2)

// COMMAND ----------


