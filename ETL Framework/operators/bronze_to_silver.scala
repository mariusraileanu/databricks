// Databricks notebook source
dbutils.widgets.text("bronzeTableName", "")
dbutils.widgets.text("silverTableName", "")
dbutils.widgets.text("pipelineName", "")
dbutils.widgets.text("silverFilePath", "")
dbutils.widgets.text("fileName", "")
dbutils.widgets.text("fileType", "")
dbutils.widgets.text("silverContainer", "")
dbutils.widgets.text("bronzeContainer", "")

// COMMAND ----------

// MAGIC %run "../utils/classes-methods-init"

// COMMAND ----------

def createSilverTable(bronzeTableName: String, silverFilePath: String, fileName: String, fileType: String, tableName: String, silverContainer: String, bronzeContainer: String) = {
    var connectionProperties = new connectToSQL(jdbcKey = "metadataSQLJDBC", usernameKey = "metadataSQLDBUsername", passwordKey = "metadataSQLDBPassword")
    val pipelineTransformationDF = spark.read.jdbc(connectionProperties.jdbcUrl, "V_PIPELINE_TRANSFORMATION", connectionProperties.connectionProperties).filter(s"PIPELINE_NAME = '$pipelineName'")

    val newColumns = LinkedHashMap[String, String]()

    case class clsPipelineTransformation(SRC_COLUMN:String, TGT_COLUMN:String, DATA_TYPE:String, TRANSFORMATION:String)
    pipelineTransformationDF.as[clsPipelineTransformation].take(pipelineTransformationDF.count.toInt).foreach(row => newColumns(s"${row.SRC_COLUMN}") = s"${row.TGT_COLUMN}")
  
    val silverFullFilePath = f"/mnt/$silverContainer/$silverFilePath/$fileName.$fileType"
    println(f"Loading data from $bronzeContainer.$bronzeTableName")    
    val df = spark.table("$bronzeContainer.$bronzeTableName").transform(renameColumns(newColumns))
    
    println(f"Saving data to $silverFullFilePath")
    df.write.format("delta").mode("overwrite").save(silverFullFilePath)
  
    // Create the table.
    spark.sql(f"CREATE TABLE IF NOT EXISTS $silverContainer.$tableName USING DELTA LOCATION '$silverFullFilePath'")
}

// COMMAND ----------

val bronzeTableName = dbutils.widgets.get("bronzeTableName")
val silverTableName = dbutils.widgets.get("silverTableName")
val pipelineName = dbutils.widgets.get("pipelineName")
val fileName = dbutils.widgets.get("fileName")
val fileType = dbutils.widgets.get("fileType")
val silverFilePath = dbutils.widgets.get("silverFilePath")
val silverContainer = dbutils.widgets.get("silverContainer")
val bronzeContainer = dbutils.widgets.get("bronzeContainer")

createSilverTable(bronzeTableName = bronzeTableName, silverFilePath = silverFilePath, fileName = fileName, fileType = fileType, tableName = tableName, silverContainer = silverContainer, bronzeContainer = bronzeContainer)
