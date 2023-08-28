# Databricks notebook source
# DBTITLE 1,Source Schema
dbutils.fs.rm("/test/project/system_mapping/source_json.json")

code_text='''
{
	"field_delimiter" : ",",
	"header" : false,
	"isFixedWidth" : false, 
	"properties" : [
		{
			"name" : "LONGITUDE",
			"type" : "FLOAT"
		},
		{
			"name" : "Country",
			"type" : "VARHCAR"
		},
		{
			"name" : "SOURCE_DATE",
			"type" : "VARCHAR"
		},
		{
			"name" : "SOURCE_TIMESTAMP",
			"type" : "VARCHAR"
		}  
    ]
}
'''

dbutils.fs.put("/test/project/system_mapping/source_json.json", code_text, True)

# COMMAND ----------

# DBTITLE 1,Target Schema
dbutils.fs.rm("/test/project/system_mapping/target_json.json")

code_text='''
{
	"properties" : [
		{
			"name" : "LONGITUDE_NEW",
			"type" : "DOUBLE"
		},
		{
			"name" : "Country",
			"type" : "VARCHAR(5)"
		},
		{
			"name" : "TARGET_DATE",
			"type" : "DATE"
		},
		{
			"name" : "TARGET_EXP",
			"type" : "VARCHAR"
		},
		{
			"name" : "TARGET_TIMESTAMP",
			"type" : "TIMESTAMP"
		},
		{
			"name" : "COUNTRY_FLAG",
			"type" : "VARCHAR"
		},
		{
			"name" : "STATIC_VALUE",
			"type" : "VARCHAR"
		}
	]
}
'''

dbutils.fs.put("/test/project/system_mapping/target_json.json", code_text, True)

# COMMAND ----------

# DBTITLE 1,Mapping
dbutils.fs.rm("/test/project/system_mapping/mapping_json.json")

code_text='''
{
	"mappings" : [
		{
		  "source" : {
			"name" : "LONGITUDE",
			"type" : "FLOAT"
		  },
		  "target" : {
			"name" : "LONGITUDE_NEW",
			"type" : "DOUBLE"
		  }
		},
		{
		  "source" : {
			"name" : "Country",
			"type" : "VARHCAR"
		  },
		  "target" : {
			"name" : "Country",
			"type" : "VARCHAR(5)"
		  }
		},
		{
		  "source" : {
			"name" : "SOURCE_DATE",
			"type" : "VARCHAR"
		  },
		  "target" : {
			"name" : "TARGET_DATE",
			"type" : "DATE",
			"expression" : "TO_DATE(SOURCE_DATE,'dd/mm/yyyy')"
		  }
		},
		{
		  "source" : {
			"name" : "",
			"type" : ""
		  },
		  "target" : {
			"name" : "TARGET_EXP",
			"type" : "VARCHAR",
			"expression" : " LONGITUDE ||','|| Country "
		  }
		},
		{
		  "source" : {
			"name" : "SOURCE_TIMESTAMP",
			"type" : "VARCHAR"
		  },
		  "target" : {
			"name" : "TARGET_TIMESTAMP",
			"type" : "TIMESTAMP",
			"expression" : "to_timestamp(SOURCE_TIMESTAMP, 'yyyy/MM/dd HH:mm:ss')"
		  }
		},
		{
		  "source" : {
			"name" : "",
			"type" : ""
		  },
		  "target" : {
			"name" : "COUNTRY_FLAG",
			"type" : "VARCHAR",
			"expression" : "substring(Country, 1, 3)"
		  }
		},
		{
		  "source" : {
			"name" : "",
			"type" : ""
		  },
		  "target" : {
			"name" : "STATIC_VALUE",
			"type" : "VARCHAR",
			"expression" : "concat('', 'Im constant string')"
		  }
		},
		{
		  "source" : {
			"name" : "",
			"type" : ""
		  },
		  "target" : {
			"name" : "dt2_business",
			"type" : "DATE",
			"expression" : "current_date()"
		  }
		}
  ]
}
'''

dbutils.fs.put("/test/project/system_mapping/mapping_json.json", code_text, True)

# COMMAND ----------

# DBTITLE 1,Data File
dbutils.fs.rm("/test/project/system_mapping/input.csv")

code_text='''
1.2,INDIA    ,12/12/2023,2022/05/10 12:30:00
2.4,AUSTRELIA,02/01/2022,2023/02/28 10:30:00
'''

dbutils.fs.put("/test/project/system_mapping/input.csv", code_text, True)

# COMMAND ----------

# DBTITLE 1,Positional Source file
dbutils.fs.rm("/test/project/system_mapping/source_json_1.json")

code_text='''
{
	"field_delimiter" : "|",
	"header" : false,
	"isFixedWidth" : true, 
	"properties" : [
		{
			"name" : "LONGITUDE",
			"type" : "FLOAT",
            "position": "1,3"
		},
		{
			"name" : "Country",
			"type" : "VARHCAR",
            "position": "5,9"
		},
		{
			"name" : "SOURCE_DATE",
			"type" : "VARCHAR",
            "position": "15,10"
		},
		{
			"name" : "SOURCE_TIMESTAMP",
			"type" : "VARCHAR",
            "position": "26,19"
		}  
    ]
}
'''

dbutils.fs.put("/test/project/system_mapping/source_json_1.json", code_text, True)

# COMMAND ----------


