// Databricks notebook source
import java.util.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import spark.implicits._
import scala.collection.mutable.LinkedHashMap

// COMMAND ----------

class connectToSQL(jdbcKey: String, usernameKey: String, passwordKey: String) {
  val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

  // Create a Properties() object to hold the parameters.
  val connectionProperties = new Properties()
  connectionProperties.setProperty("Driver", driverClass)

  val jdbcUrl = dbutils.secrets.get(scope = "key-vault-secrets", key = jdbcKey)

  val jdbcUsername = dbutils.secrets.get(scope = "key-vault-secrets", key = usernameKey)
  val jdbcPassword = dbutils.secrets.get(scope = "key-vault-secrets", key = passwordKey)

  connectionProperties.put("user", s"${jdbcUsername}")
  connectionProperties.put("password", s"${jdbcPassword}")
}

// COMMAND ----------

def renameColumns(newColumns: LinkedHashMap[String, String])(df: DataFrame): DataFrame = {  
  df.select(newColumns.map(x => col(x._1).alias(x._2)).toList : _*)
}

// COMMAND ----------

//dbutils.notebook.exit("All classes and methods successfully loaded.")
