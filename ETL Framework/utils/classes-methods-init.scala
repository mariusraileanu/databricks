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

def createBronzeTable(rawFilePath: String, bronzeFilePath: String, fileName: String, fileType: String, tableName: String, rawContainer: String, bronzeContainer: String) = {
    val bronzeFullFilePath = f"/mnt/$bronzeContainer/$bronzeFilePath/$fileName.$fileType"
    val rawFullFilePath = f"/mnt/$rawContainer/$rawFilePath/$fileName.$fileType"
    println(f"Loading data from $rawFullFilePath")    
    val df = spark.read.parquet(rawFullFilePath)
    
    println(f"Saving data to $bronzeFullFilePath")
    df.write.format("delta").mode("overwrite").save(bronzeFullFilePath)
  
    // Create the table.
    spark.sql(f"CREATE TABLE IF NOT EXISTS bronze.$tableName USING DELTA LOCATION '$bronzeFullFilePath'")
}

// COMMAND ----------

case class clsPipelineTransformation(SRC_COLUMN:String, TGT_COLUMN:String, DATA_TYPE:String, TRANSFORMATION:String)

def createSilverTable(bronzeTableName: String, silverFilePath: String, fileName: String, fileType: String, silverTableName: String, silverContainer: String, bronzeContainer: String, pipelineName: String) = {
    var connectionProperties = new connectToSQL(jdbcKey = "metadataSQLJDBC", usernameKey = "metadataSQLDBUsername", passwordKey = "metadataSQLDBPassword")
    val pipelineTransformationDF = spark.read.jdbc(connectionProperties.jdbcUrl, "V_PIPELINE_TRANSFORMATION", connectionProperties.connectionProperties).filter(s"PIPELINE_NAME = '$pipelineName'")

    val newColumns = LinkedHashMap[String, String]()
    pipelineTransformationDF.as[clsPipelineTransformation].take(pipelineTransformationDF.count.toInt).foreach(row => newColumns(s"${row.SRC_COLUMN}") = s"${row.TGT_COLUMN}")
  
    val silverFullFilePath = f"/mnt/$silverContainer/$silverFilePath/$fileName.$fileType"
    println(f"Loading data from $bronzeContainer.$bronzeTableName")    
    val df = spark.table(f"$bronzeContainer.$bronzeTableName").transform(renameColumns(newColumns))
    
    println(f"Saving data to $silverFullFilePath")
    df.write.format("delta").mode("overwrite").save(silverFullFilePath)
  
    // Create the table.
    spark.sql(f"CREATE TABLE IF NOT EXISTS $silverContainer.$silverTableName USING DELTA LOCATION '$silverFullFilePath'")
}

// COMMAND ----------

//dbutils.notebook.exit("All classes and methods successfully loaded.")
