# Databricks notebook source
# CREATE OR REPLACE TABLE STAGING_STORES (store_id varchar(5), location varchar(100), value integer)
# ;

# CREATE TABLE FINAL_STORES (store_id varchar(5), location varchar(100), value integer, start_date date, end_date date, active_flag char)
# ;

# CREATE OR REPLACE FILE FORMAT CSV_FORMAT
#     TYPE = CSV
#     FIELD_DELIMITER = ','
#     SKIP_HEADER = 1
#     NULL_IF = ('NULL', 'NULL')
#     EMPTY_FIELD_AS_NULL = TRUE
#     COMPRESSION = GZIP
# ;

# CREATE STAGE CSV_STAGE
#     DIRECTORY = (ENABLE = TRUE)
#     FILE_FORMAT = CSV_FORMAT
# ;

# --put file://D:\Datasets\stores.csv @CSV_STAGE;

# SELECT T.$1, T.$2, T.$3 FROM @CSV_STAGE (FILE_FORMAT => 'CSV_FORMAT') T;

# INSERT INTO STAGING_STORES SELECT T.$1, T.$2, T.$3 FROM @CSV_STAGE (FILE_FORMAT => 'CSV_FORMAT') T;

# SELECT * FROM STAGING_STORES;

# COMMAND ----------

sfConnection = {
    "sfURL" : "https://ryyplfk-fg21385.snowflakecomputing.com",
    "sfUser" : "ak926463",
    "sfPassword" : "xmb#529Geq",
    "sfDatabase" : "TEST_DB",
    "sfSchema" : "TEST_SCHEMA",
    "sfRole" : "accountadmin",
    "sfWarehouse" : "MY_WH"
}

sfConnection

# COMMAND ----------

sourceTable = spark.read \
    .format("snowflake") \
    .options(**sfConnection) \
    .option("dbTable", "STAGING_STORES") \
    .load()

sourceTable.show()

# COMMAND ----------

targetTable = spark.read \
    .format("snowflake") \
    .options(**sfConnection) \
    .option("dbTable", "FINAL_STORES") \
    .load()

targetTable.show()

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import *

class scdProcess:
    def __init__(self, source_table, target_table):
        self.runControlDate = None
        self.keyColumns = []
        self.scdActiveflagColumnName = ''
        self.scdEndDateColumnName = None
        self.scdStartDateColumnName = None
        self.scdActiveEndDate = None
        self.targetTable = self.read_batch_dataframe(target_table)
        self.sourceTable = self.read_batch_dataframe(source_table)
        self.error_message = ''
        self.refreshType = None
        self.sourceColumnArray = sorted(self.read_batch_dataframe(source_table).schema.names)
        self.targetColumnArray = sorted(self.read_batch_dataframe(target_table).schema.names)

    def read_batch_dataframe(self, table_name):
        df = spark.read \
                    .format("snowflake") \
                    .options(**sfConnection) \
                    .option("dbTable", table_name) \
                    .load()
        return df

    def validate_params(self):
        target_column_compare_array = []
        maxEndDateRow = None

        # print("Target DF : ")
        # print(self.targetTable)
        # print("Source DF : ")
        # print(self.sourceTable)
        # print("sourceColumnArray : " + ','.join(self.sourceColumnArray))
        # print("targetColumnArray : " + ','.join(self.targetColumnArray))
        # print("runControlDate : " + self.runControlDate)
        # print("keyColumns : " + ','.join(self.keyColumns))
        # print("scdActiveFlagColumnName : " + self.scdActiveflagColumnName)
        # print("scdEndDateColumnName : " + self.scdEndDateColumnName)
        # print("scdStartDateColumnName : " + self.scdStartDateColumnName)
        # print("scdActiveEndDate : " + self.scdActiveEndDate)
        # print("refreshType : " + self.refreshType)

        target_scd_column_array = [self.scdStartDateColumnName, self.scdEndDateColumnName, self.scdActiveflagColumnName]

        if not self.sourceColumnArray:
            self.error_message += f" Cannot retrieve Column List for Source Table ({self.sourceTable})."
        if not self.targetColumnArray:
            self.error_message += f" Cannot retrieve Column List for Source Table ({self.targetTable})."

        if not self.scdStartDateColumnName.strip():
            self.error_message += f" Audit Column (self.scdStartDateColumnName) not passed for Target table ({self.targetTable})."

        if not self.scdEndDateColumnName.strip():
            self.error_message += f" Audit Column (scdStartDateColumnName) not passed for Target table ({self.targetTable})."

        if self.targetColumnArray:
            if self.scdStartDateColumnName not in self.targetColumnArray:
                self.error_message += f" Target Table ({self.targetTable}) has missing column {self.scdStartDateColumnName}."
            if self.scdEndDateColumnName not in self.targetColumnArray:
                self.error_message += f" Target Table ({self.targetTable}) has missing column {self.scdEndDateColumnName}."

        if self.sourceColumnArray and self.targetColumnArray:
            if not self.keyColumns:
                self.error_message += f" Key Column(s) ({' '.join(self.keyColumns)}) required."
            elif len(self.keyColumns) > 5:
                self.error_message += f"Distinct Key Columns(s) ({' '.join(set(self.keyColumns))}) > 5 not supported. Contact Developer's group for expansion."
            else:
                cnt_target, cnt_source = 0, 0

                for colmn in self.keyColumns:
                    if colmn not in self.targetColumnArray:
                        cnt_target += 1

                for colmn in self.keyColumns:
                    if colmn not in self.sourceColumnArray:
                        cnt_source += 1

                if cnt_target:
                    self.error_message += f" All key Column(s) ({' '.join(self.keyColumns)}) not found in Target table " \
                                        f"columns {' '.join(self.targetColumnArray)}. "
                if cnt_source:
                    self.error_message += f" All key column(s) ({' '.join(self.keyColumns)}) not found in Source table " \
                                        f"columns {' '.join(self.sourceColumnArray)}. "

        for colmn in self.targetColumnArray:
            if colmn not in target_scd_column_array:
                target_column_compare_array.append(colmn)

        if self.sourceColumnArray != target_column_compare_array:
            set_dif_source = set(self.sourceColumnArray).difference(set(target_column_compare_array))
            set_dif_target = set(target_column_compare_array).difference(set(self.sourceColumnArray))
            set_dif = set_dif_source.union(set_dif_target)

            self.error_message += f" Column list mismatched between source ({self.sourceTable}) and target ({self.targetTable}). Found in one but not in other : {' '.join(set_dif)} . Source Column List: {' '.join(self.sourceColumnArray)} Target Column List: {' '.join(self.targetColumnArray)} ."
        elif not self.error_message:
            max_start_date_row = self.targetTable.agg(max(col(self.scdStartDateColumnName))).take(1)[0][0]
            if ((max_start_date_row is not None) and (str(max_start_date_row)[:10])) > self.runControlDate:
                self.error_message += f" Run Control Date parameter ({self.runControlDate}) cannot be earlier \
                than the latest Start Date ({max_start_date_row[:10]}) on Target Table / Column ({self.targetTable}/ {self.scdStartDateColumnName}). "
            maxEndDateRow = self.targetTable.agg(max(col(self.scdEndDateColumnName))).take(1)[0][0]

        # Add condition here: col(self.scdEndDateColumnName) == F.lit(maxEndDateRow)
        if self.targetTable.filter(col(self.scdEndDateColumnName).isNotNull() | (
                col(self.scdEndDateColumnName) == lit(maxEndDateRow))).groupBy(self.keyColumns).count().filter(col("count") > 1).count() > 0:
            self.error_message += "Duplicates found based on Key Columns (" + ",".join(
                self.keyColumns) + ") in target table " + self.targetTable

        if self.sourceTable.groupBy(self.keyColumns).count().filter(col("count") > 1).count() > 0:
            self.error_message += "Duplicates found based on Key Columns (" + ",".join(
                self.keyColumns) + ") in source table " + self.sourceTable

        print("maxEndDateRow : " + str(maxEndDateRow))

        if maxEndDateRow is not None and self.scdActiveEndDate is not None and str(
                maxEndDateRow) > self.scdActiveEndDate:
            self.error_message += "scdActiveEndDate parameter (" + self.scdActiveEndDate + ") cannot be earlier than " \
                                                                                        "latest End Date (" + str(
                maxEndDateRow) + ") on Target Table / Column (" + self.targetTable + "/ " + self.scdEndDateColumnName \
                                + "). "
        return self.error_message, target_column_compare_array
    
    def run(self
            , keyColumns: list
            , scdStartDateColumnName: str
            , scdEndDateColumnName: str
            , scdActiveflagColumnName: str
            , refreshType: str
            , runControlDate: str
            , scdActiveEndDate: str) -> DataFrame:

            return_code = 0
            #try:
            commonColumnArray = []
            targetColumnOrderArray = []

            # Convert column names to uppercase
            self.keyColumns = [c.upper() for c in keyColumns]
            self.scdStartDateColumnName = scdStartDateColumnName.upper()
            self.scdEndDateColumnName = scdEndDateColumnName.upper()
            self.scdActiveflagColumnName = scdActiveflagColumnName.upper()
            self.scdActiveEndDate = scdActiveEndDate
            self.refreshType = refreshType.lower().strip()
            self.runControlDate = runControlDate if runControlDate and runControlDate.strip() else datetime.today().strftime('%Y-%m-%d')

            # Validation
            validation_error_message, validation_commonColumnArray = self.validate_params()
            print("validation_error_message : " + validation_error_message)
            print("validation_commonColumnArray : " + ','.join(validation_commonColumnArray))

            self.error_message = validation_error_message
            commonColumnArray = validation_commonColumnArray
            if len(self.error_message.strip()) == 0:
                commonColumnString = ",".join(commonColumnArray)
                targetColumnOrderArray = self.targetColumnArray
                print(commonColumnString)
                print(targetColumnOrderArray)

                #Load source and target tables
                dfSource = self.sourceTable.withColumn("SOURCE_HASH_ROW_VALUE", hash(concat_ws(",", *self.sourceTable.select(commonColumnString.split(',')))))
                dfTarget = self.targetTable.withColumn("TARGET_HASH_ROW_VALUE", hash(concat_ws(",", *self.targetTable.select(commonColumnString.split(',')))))
                
                print("=====================================================================================================")
                dfSource.show()

                dfTarget.show()

                # Filter active records in target table
                tmp_scdActiveDate = self.scdActiveEndDate if self.scdActiveEndDate else f"'{self.scdActiveEndDate}'" 
                
                dfTargetActive = dfTarget.filter((col(scdEndDateColumnName).isNull()) | (
                        coalesce(col(scdEndDateColumnName), lit('1900-01-01')) == coalesce(to_date(lit(tmp_scdActiveDate)),
                                                                                        lit('1901-01-01'))))
                print("\n dfTargetActive ;")
                dfTargetActive.show()

                # Filter empty hash row value in target table
                dfTargetEmpty = dfTargetActive.filter(col("TARGET_HASH_ROW_VALUE") == 0)
                print("\n dfTargetEmpty ;")
                dfTargetEmpty.show()

                dfTargetActive.printSchema()

                # Update records in source table
                dfSourceUpdates = dfSource.join(dfTargetActive, keyColumns, "leftsemi") \
                    .filter(
                        (dfSource["SOURCE_HASH_ROW_VALUE"] != dfTargetActive["TARGET_HASH_ROW_VALUE"]) \
                        & (
                            dfTargetActive[scdEndDateColumnName].isNull() | (
                                coalesce(dfTargetActive[scdEndDateColumnName], to_date(lit('1901-01-01'))) == coalesce(to_date(lit(tmp_scdActiveDate)), to_date(lit('1901-01-01')))
                            )
                        )
                    )
                    
                    #.filter(~(dfSource["SOURCE_HASH_ROW_VALUE"] == dfTargetActive["TARGET_HASH_ROW_VALUE"]))
                            # & ((dfTargetActive[scdEndDateColumnName].isNull())
                            # | (coalesce(dfTargetActive[scdEndDateColumnName], to_date(lit('1901-01-01'))) == coalesce(
                            # to_date(lit(tmp_scdActiveDate)), to_date(lit('1901-01-01')))))) \
                            

                print("\n dfSourceUpdates ;")
                dfSourceUpdates.show()

            # except Exception as e:
            #     print(e)

# COMMAND ----------

obj = scdProcess(source_table="STAGING_STORES"
        , target_table="FINAL_STORES")

obj.run(keyColumns=["store_id"]
        , scdStartDateColumnName="start_date"
        , scdEndDateColumnName="end_date"
        , scdActiveflagColumnName="active_flag"
        , refreshType="delta"
        , runControlDate="2023-05-10"
        , scdActiveEndDate="9999-12-31"
    );

# COMMAND ----------


