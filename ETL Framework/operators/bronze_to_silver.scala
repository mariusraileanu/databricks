// Databricks notebook source
dbutils.widgets.text("rawFilePath", "")
dbutils.widgets.text("bronzeFilePath", "")
dbutils.widgets.text("fileName", "")
dbutils.widgets.text("fileType", "")
dbutils.widgets.text("tableName", "")
dbutils.widgets.text("rawContainer", "")
dbutils.widgets.text("bronzeContainer", "")

// COMMAND ----------

val rawFilePath = dbutils.widgets.get("rawFilePath")
val bronzeFilePath = dbutils.widgets.get("bronzeFilePath")
val fileName = dbutils.widgets.get("fileName")
val fileType = dbutils.widgets.get("fileType")
val tableName = dbutils.widgets.get("tableName")
val rawContainer = dbutils.widgets.get("rawContainer")
val bronzeContainer = dbutils.widgets.get("bronzeContainer")

// COMMAND ----------

// MAGIC %run "/ETL Framework/utils/classes-methods-init"

// COMMAND ----------

var connectionProperties = new connectToSQL(jdbcKey = "metadataSQLJDBC", usernameKey = "metadataSQLDBUsername", passwordKey = "metadataSQLDBPassword")
val pipelineTransformationDF = spark.read.jdbc(connectionProperties.jdbcUrl, "PIPELINE_TRANSFORMATION", connectionProperties.connectionProperties)

val newColumns = LinkedHashMap[String, String]()

case class clsPipelineTransformation(SRC_COLUMN:String, TGT_COLUMN:String, DATA_TYPE:String, TRANSFORMATION:String)
pipelineTransformationDF.as[clsPipelineTransformation].take(pipelineTransformationDF.count.toInt).foreach(row => newColumns(s"${row.SRC_COLUMN}") = s"${row.TGT_COLUMN}")

// COMMAND ----------

val rawFilePath = "/mnt/raw/data/source=mysql/dataset=customer/2022/03/29/customer.parquet"
val df = spark.read.parquet(rawFilePath).transform(renameColumns(newColumns))
  .show()
