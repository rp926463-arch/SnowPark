// Databricks notebook source
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.SaveMode
import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._
import scala.io._
import java.io._

class BankingDataSF {
  
  def write_to_stagging_table(data_file: String): Unit = {
    var df2 = spark.read.options(Map("inferSchema"->"true","header"->"true")).csv(data_file)
    df2.write
        .format("snowflake")
        .options(get_conn_details("sf", "STAGING"))
        .option("dbtable", "EMPLOYEE")
        .mode(SaveMode.Overwrite)
        .save()
  }
  
  def get_conn_details(pref: String, schema: String = "PUBLIC"): Map[String, String] = {
    var sfOptions = Map(
      pref+"url" -> "xj21352.central-india.azure.snowflakecomputing.com",
      pref+"user" -> "****",
      pref+"password" -> "****",
      pref+"role" -> "accountadmin",
      pref+"warehouse" -> "COMPUTE_WH",
      pref+"database" -> "TESTDB",
      pref+"schema" -> schema
    )
    sfOptions
  }
  val session = Session.builder.configs(get_conn_details("")).create
  
  def get_select_statement(ar_map: Map[String, List[Map[String, Map[String, String]]]], table: String, lst: Seq[String]): String = {
    var (fl_st,st) = ("","")
    var lst_var = lst
    ar_map("mappings").foreach{ itm => 
      if (itm("target").contains("type")){
        st = itm("source")("name")+" :: "+itm("target")("type")+" "+itm("target")("name")+","
      }
      else{
        st = itm("source")("name")+" "+itm("target")("name")+","
      }
      fl_st = fl_st + st
      if(lst_var.contains(itm("source")("name"))){
        lst_var = lst_var.filter(_ != itm("source")("name"))
      }
    }
    "SELECT "+fl_st+" "+lst_var.mkString(",")+" FROM "+table
  }
  
  def map_source_tble_to_target_tbl(mapping_file_path: String, src_table_name: String, trg_table_name: String): Unit = {
    import com.snowflake.snowpark.SaveMode
    try{
      val json = Source.fromFile("/dbfs/FileStore/tables/mapping.json")
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      val jsonMap = mapper.readValue[Map[String, Object]](json.reader())

      var parsedJson = jsonMap.asInstanceOf[Map[String, List[Map[String, Map[String, String]]]]]

      var df = session.table(src_table_name)
      var final_df = session.sql(get_select_statement(parsedJson, src_table_name, df.schema.map(_.name)))

      final_df.write.mode(SaveMode.Append).option("columnOrder", "name").saveAsTable(trg_table_name)
    }
    catch{
      case x: FileNotFoundException => println("Exception: Json Mapping file missing")
      case y: IOException => println("IOException occurred")
      case e: Exception => println(e)
    }
  }

  def run(): Unit = {
    var data_file = "/FileStore/tables/california_housing_train.csv"
    var mapping_file = "/dbfs/FileStore/tables/mapping.json"
    write_to_stagging_table(data_file)
    map_source_tble_to_target_tbl(mapping_file, "EMPLOYEE", "SHUFFLED_EMPLOYEE")
    session.close()
  }
}
object Driver extends App{
  def main(): Unit = {
    val obj = new BankingDataSF()
    obj.run()
  }
}

// COMMAND ----------

Driver.main

// COMMAND ----------


