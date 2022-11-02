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

val bronzeTableName = dbutils.widgets.get("bronzeTableName")
val silverTableName = dbutils.widgets.get("silverTableName")
val pipelineName = dbutils.widgets.get("pipelineName")
val fileName = dbutils.widgets.get("fileName")
val fileType = dbutils.widgets.get("fileType")
val silverFilePath = dbutils.widgets.get("silverFilePath")
val silverContainer = dbutils.widgets.get("silverContainer")
val bronzeContainer = dbutils.widgets.get("bronzeContainer")

createSilverTable(bronzeTableName = bronzeTableName, silverFilePath = silverFilePath, fileName = fileName, fileType = fileType, silverTableName = silverTableName, silverContainer = silverContainer, bronzeContainer = bronzeContainer, pipelineName = pipelineName)
