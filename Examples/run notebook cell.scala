// Databricks notebook source
val notebookPath = "notebook cell logic"
val notebookTimeout = 60
val notebookParameters = Map("cellNumber" -> "2")

dbutils.notebook.run(notebookPath, notebookTimeout, notebookParameters)
