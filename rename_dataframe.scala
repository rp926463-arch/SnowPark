//Get Mapping from scala
import scala.io._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


val json = Source.fromFile("/dbfs/FileStore/tables/test_data.json")
val mapper = new ObjectMapper() with ScalaObjectMapper
mapper.registerModule(DefaultScalaModule)
var jsonMap = mapper.readValue[Map[String, Object]](json.reader())

//convert mapping Obj to MAP
var parsedJson = jsonMap.asInstanceOf[Map[String, List[Map[String,Map[String,String]]]]]

//get target mapping
def get_target_name(source_fld_name: String) : String = {
  var res = ""
  parsedJson("mappings").foreach( lst => {
    if(lst("source")("name") == source_fld_name){
      res = lst("target")("name")
    }
  })
  res
}

//CREATE TEST DF
import spark.implicits._
val columns = Seq("language","users_count")
val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
val rdd = spark.sparkContext.parallelize(data)
val dfFromRDD1 = rdd.toDF("language","users_count")

//Create Map of mapping
var m1 = collection.mutable.Map[String, String]()
dfFromRDD1.columns.foreach{ clmn => m1 += (clmn -> get_target_name(clmn)) }

//Rename DF
dfFromRDD1.select(dfFromRDD1.columns.map(c => dfFromRDD1(c).alias(m1.get(c).getOrElse(c))): _*).show()