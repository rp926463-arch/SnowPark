// Databricks notebook source
package shorty

import org.apache.log4j._

/** Wraps log4j in a trait that not only makes it easy to log things
  * but also uses lazy evaluation for log messages -- no need to 
  * wrap them in an if statement for expensive messages
  *
  * <pre>
  * class Foo extends Logs {
  *   def doit = {
  *     debug("This is a " + very.complicated + " log message")
  *     fatal(someException)
  *     // etc.
  *   }
  * }
  * </pre>
  */
trait Logs {
  private[this] val logger = Logger.getLogger(getClass().getName());

  import org.apache.log4j.Level._

  def debug(message: => String) = if (logger.isEnabledFor(DEBUG)) logger.debug(message)
  def debug(message: => String, ex:Throwable) = if (logger.isEnabledFor(DEBUG)) logger.debug(message,ex)
  def debugValue[T](valueName: String, value: => T):T = {
    val result:T = value
    debug(valueName + " == " + result.toString)
    result
  }

  def info(message: => String) = if (logger.isEnabledFor(INFO)) logger.info(message)
  def info(message: => String, ex:Throwable) = if (logger.isEnabledFor(INFO)) logger.info(message,ex)

  def warn(message: => String) = if (logger.isEnabledFor(WARN)) logger.warn(message)
  def warn(message: => String, ex:Throwable) = if (logger.isEnabledFor(WARN)) logger.warn(message,ex)

  def error(ex:Throwable) = if (logger.isEnabledFor(ERROR)) logger.error(ex.toString,ex)
  def error(message: => String) = if (logger.isEnabledFor(ERROR)) logger.error(message)
  def error(message: => String, ex:Throwable) = if (logger.isEnabledFor(ERROR)) logger.error(message,ex)

  def fatal(ex:Throwable) = if (logger.isEnabledFor(FATAL)) logger.fatal(ex.toString,ex)
  def fatal(message: => String) = if (logger.isEnabledFor(FATAL)) logger.fatal(message)
  def fatal(message: => String, ex:Throwable) = if (logger.isEnabledFor(FATAL)) logger.fatal(message,ex)
}

// COMMAND ----------

import com.snowflake.snowpark._

import shorty.Logs

//Imported for adjusting the logging level. 

import org.slf4j.{Logger, LoggerFactory}
var feedSourceCode = "abc"
var runControlDate = "2023-02-25"
 
val customLogs = LoggerFactory.getLogger("CustomLogs-" + feedSourceCode + "-" + runControlDate)

/**
 * Connects to a Snowflake database and prints a list of tables in the database to the console.
 *
 * You can use this class to verify that you set the connection properties correctly in the
 * snowflake_connection.properties file that is used by this code to create a session.
 */
object HelloWorld extends Logs{
  def main(args: Array[String]): Unit = {
    // By default, the library logs INFO level messages.
    // If you need to adjust the logging levels, uncomment the statement below, and change X to
    // the level that you want to use.
    // (https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/Level.html)
    customLogs.info("com.snowflake.snowpark")
    
    var sfOptions = Map(
      "url" -> "ljxubxq-bt83120.snowflakecomputing.com",
      "user" -> "***",
      "password" -> "***",
      "role" -> "accountadmin",
      "warehouse" -> "COMPUTE_WH",
      "database" -> "SNOWFLAKE_SAMPLE_DATA",
      "schema" -> "TPCH_SF1"
    )
    
    customLogs.info("Looking up sfOptions " + sfOptions)
 
    Console.println("\n=== Creating the session ===\n")
    // Create a Session that connects to a Snowflake deployment.
    val session = Session.builder.configs(sfOptions).create

    Console.println("\n=== Creating a DataFrame to execute a SQL statement ===\n")
    // Create a DataFrame that is set up to execute the SHOW TABLES command.
    val df = session.sql("show tables")

    Console.println("\n=== Execute the SQL statement and print the first 10 rows ===\n")
    // Execute the SQL statement and print the first 10 rows returned in the output.
    df.show()
  }
}


// COMMAND ----------

HelloWorld.main(Array())

// COMMAND ----------


