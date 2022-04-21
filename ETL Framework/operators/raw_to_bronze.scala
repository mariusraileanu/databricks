// Databricks notebook source
dbutils.widgets.text("rawFilePath", "")
dbutils.widgets.text("bronzeFilePath", "")
dbutils.widgets.text("fileName", "")
dbutils.widgets.text("fileType", "")
dbutils.widgets.text("tableName", "")
dbutils.widgets.text("rawContainer", "")
dbutils.widgets.text("bronzeContainer", "")

// COMMAND ----------

// MAGIC %run "../utils/classes-methods-init"

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

val rawFilePath = dbutils.widgets.get("rawFilePath")
val bronzeFilePath = dbutils.widgets.get("bronzeFilePath")
val fileName = dbutils.widgets.get("fileName")
val fileType = dbutils.widgets.get("fileType")
val tableName = dbutils.widgets.get("tableName")
val rawContainer = dbutils.widgets.get("rawContainer")
val bronzeContainer = dbutils.widgets.get("bronzeContainer")

createBronzeTable(rawFilePath = rawFilePath, bronzeFilePath = bronzeFilePath, fileName = fileName, fileType = fileType, tableName = tableName, rawContainer = rawContainer, bronzeContainer = bronzeContainer)
