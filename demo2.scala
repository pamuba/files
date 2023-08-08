// Databricks notebook source
// MAGIC %scala
// MAGIC // Databricks notebook source
// MAGIC val containerName = "staging"
// MAGIC val storageAccountName = "adbmarlabsdemo"
// MAGIC val sas = "?sv=2022-11-02&ss=b&srt=sco&sp=rwdlaciytfx&se=2023-08-08T20:00:26Z&st=2023-08-08T12:00:26Z&spr=https&sig=oBYWq0nMVg2hzmVn7VLdnZlrT7zYgtl4uz%2BLQtPoeQs%3D"
// MAGIC
// MAGIC val url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
// MAGIC var config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"
// MAGIC

// COMMAND ----------

// MAGIC %scala
// MAGIC dbutils.fs.mount(
// MAGIC   source = url,
// MAGIC   mountPoint = "/mnt/staging",
// MAGIC   extraConfigs = Map(config -> sas))

// COMMAND ----------

val df = spark.read.json("/mnt/staging/small_radio_json.json")
display(df)

// COMMAND ----------

val specificColumnsDf = df.select("firstname", "lastname", "gender", "location", "level")
display(specificColumnsDf)

// COMMAND ----------

val renamedColumnsDF = specificColumnsDf.withColumnRenamed("level", "subscription_type")
display(renamedColumnsDF)

// COMMAND ----------

renamedColumnsDF.createOrReplaceTempView("renamed")


// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC count(*) as count,
// MAGIC subscription_type
// MAGIC FROM renamed
// MAGIC GROUP BY subscription_type

// COMMAND ----------

val aggregate = spark.sql("""
SELECT 
   count(*) as count,
   subscription_type
FROM renamed
GROUP BY 
   subscription_type
""")

// COMMAND ----------

aggregate.write.mode("overwrite").csv("/mnt/staging/output/aggregate.csv")
