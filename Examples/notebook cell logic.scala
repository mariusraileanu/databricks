// Databricks notebook source
// global config
dbutils.widgets.text("cellNumber", "1") // cell number to execute

// COMMAND ----------

val cellNumber = dbutils.widgets.get("cellNumber")

// COMMAND ----------

if (cellNumber == "1") {
    print(f"Executing cell $cellNumber")
}

// COMMAND ----------

if (cellNumber == "2") {
    print(f"Executing cell $cellNumber")
}

// COMMAND ----------

if (cellNumber == "3") {
    print(f"Executing cell $cellNumber")
}
