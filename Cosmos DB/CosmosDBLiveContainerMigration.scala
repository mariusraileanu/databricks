// Databricks notebook source
// global config
dbutils.widgets.text("cosmosEndpoint", "") // enter the Cosmos DB Account URI of the source account
dbutils.widgets.text("cosmosMasterKey", "") // enter the Cosmos DB Account PRIMARY KEY of the source account
dbutils.widgets.text("cosmosRegion", "") // enter the Cosmos DB Region

// source config
dbutils.widgets.text("cosmosSourceDatabaseName", "") // enter the name of your source database
dbutils.widgets.text("cosmosSourceContainerName", "") // enter the name of the container you want to migrate
dbutils.widgets.text("cosmosSourceContainerThroughputControl", "") // targetThroughputThreshold defines target percentage of available throughput you want the migration to use

// target config
dbutils.widgets.text("cosmosTargetDatabaseName", "") // enter the name of your target database
dbutils.widgets.text("cosmosTargetContainerName", "") // enter the name of the target container
dbutils.widgets.text("cosmosTargetContainerPartitionKey", "") // enter the partition key for how data is stored in the target container
dbutils.widgets.text("cosmosTargetContainerProvisionedThroughput", "") // enter the partition key for how data is stored in the target container

// COMMAND ----------

val cosmosEndpoint = dbutils.widgets.get("cosmosEndpoint")
val cosmosMasterKey = dbutils.widgets.get("cosmosMasterKey")
val cosmosRegion = dbutils.widgets.get("cosmosRegion")

val cosmosSourceDatabaseName = dbutils.widgets.get("cosmosSourceDatabaseName")
val cosmosSourceContainerName = dbutils.widgets.get("cosmosSourceContainerName") 
val cosmosSourceContainerThroughputControl = dbutils.widgets.get("cosmosSourceContainerThroughputControl")

val cosmosTargetDatabaseName = dbutils.widgets.get("cosmosTargetDatabaseName")
val cosmosTargetContainerName = dbutils.widgets.get("cosmosTargetContainerName")
val cosmosTargetContainerPartitionKey = dbutils.widgets.get("cosmosTargetContainerPartitionKey")
val cosmosTargetContainerProvisionedThroughput = dbutils.widgets.get("cosmosTargetContainerProvisionedThroughput")

// COMMAND ----------

spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cosmosEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cosmosMasterKey)

// COMMAND ----------

/* NOTE: It is important to enable TTL (can be off/-1 by default) on the throughput control container */

val cosmosTargetDatabaseOptions = if (cosmosTargetContainerProvisionedThroughput contains "shared") 
                                        s"""WITH DBPROPERTIES (
                                                                ${cosmosTargetContainerProvisionedThroughput.replace("sharedDB", "")}
                                                               )""" 
                                  else 
                                        ""

val cosmosTargetContainerOptions = if (cosmosTargetContainerProvisionedThroughput contains "shared") 
                                        ""
                                   else 
                                        s"""${cosmosTargetContainerProvisionedThroughput},"""

spark.sql(s"""
              CREATE TABLE IF NOT EXISTS cosmosCatalog.`${cosmosSourceDatabaseName}`.`${cosmosSourceContainerName}_ThroughputControl` USING cosmos.oltp 
              OPTIONS (
                      spark.cosmos.database = '${cosmosSourceDatabaseName}'
                      ) 
              TBLPROPERTIES(
                              partitionKeyPath = '${cosmosTargetContainerPartitionKey}',
                              autoScaleMaxThroughput = '2000',
                              indexingPolicy = 'AllProperties',
                              defaultTtlInSeconds = '-1'
                              );
          """)

spark.sql(s"""
              CREATE DATABASE IF NOT EXISTS cosmosCatalog.`${cosmosTargetDatabaseName}`
              ${cosmosTargetDatabaseOptions};
           """)

spark.sql(s"""
              CREATE TABLE IF NOT EXISTS cosmosCatalog.`${cosmosTargetDatabaseName}`.`${cosmosTargetContainerName}` USING cosmos.oltp 
              TBLPROPERTIES (
                              partitionKeyPath = '${cosmosTargetContainerPartitionKey}',
                              ${cosmosTargetContainerOptions}
                              indexingPolicy = 'OnlySystemProperties'
                              );
           """)

// COMMAND ----------

 val changeFeedConfig = Map(
   "spark.cosmos.accountEndpoint" -> cosmosEndpoint,
   "spark.cosmos.applicationName" -> s"${cosmosSourceDatabaseName}_${cosmosSourceContainerName}_LiveMigrationRead_",
   "spark.cosmos.accountKey" -> cosmosMasterKey,
   "spark.cosmos.database" -> cosmosSourceDatabaseName,
   "spark.cosmos.container" -> cosmosSourceContainerName,
   "spark.cosmos.read.partitioning.strategy" -> "Default",
   "spark.cosmos.read.inferSchema.enabled" -> "false",   
   "spark.cosmos.changeFeed.startFrom" -> "Beginning",
   "spark.cosmos.changeFeed.mode" -> "Incremental",
   "spark.cosmos.changeFeed.itemCountPerTriggerHint" -> "50000", 
   "spark.cosmos.throughputControl.enabled" -> "true",
   "spark.cosmos.throughputControl.name" -> "SourceContainerThroughputControl",
   "spark.cosmos.throughputControl.targetThroughputThreshold" -> cosmosSourceContainerThroughputControl, 
   "spark.cosmos.throughputControl.globalControl.database" -> cosmosSourceDatabaseName,
   "spark.cosmos.throughputControl.globalControl.container" -> "ThroughputControl",
   "spark.cosmos.preferredRegionsList" -> cosmosRegion
  )

 // when running this notebook is stopped (or if a problem causes a crash) change feed processing will be picked up from last processed document
 // if you want to start from beginning, delete this folder or change checkpointLocation value
 val checkpointLocation = f"/tmp/${cosmosSourceDatabaseName}/${cosmosSourceContainerName}/LiveMigration_checkpoint"

 val writeConfig = Map(
   "spark.cosmos.accountEndpoint" -> cosmosEndpoint,
   "spark.cosmos.accountKey" -> cosmosMasterKey,
   "spark.cosmos.applicationName" -> s"${cosmosSourceDatabaseName}_${cosmosSourceContainerName}_",                     
   "spark.cosmos.database" -> cosmosTargetDatabaseName,
   "spark.cosmos.container" -> cosmosTargetContainerName,
   "spark.cosmos.write.strategy" -> "ItemOverwrite",
   "checkpointLocation" -> checkpointLocation
  )

// COMMAND ----------

val changeFeedDF = spark.readStream.format("cosmos.oltp.changeFeed")
      .options(changeFeedConfig)  
      .load
/*this will preserve the source document fields and retain the "_etag" and "_ts" property values as "_origin_etag" and "_origin_ts" in the sink documnet*/
val withAuditFieldsDF = changeFeedDF.withColumnRenamed("_rawbody", "_origin_rawBody")

// COMMAND ----------

val runId = java.util.UUID.randomUUID.toString;
    withAuditFieldsDF.writeStream
      .format("cosmos.oltp")
      .queryName(runId)
      .options(writeConfig)
      .outputMode("append")
      .start()
