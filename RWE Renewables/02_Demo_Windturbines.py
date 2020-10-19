# Databricks notebook source
# MAGIC %md Our dataset consists of vibration readings coming off sensors located in the gearboxes of wind turbines. We will use Gradient Boosted Tree Classifier to predict which set of vibrations could be indicative of a failure.
# MAGIC 
# MAGIC *See image below for locations of the sensors*
# MAGIC 
# MAGIC https://www.nrel.gov/docs/fy12osti/54530.pdf
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/databricks-demo-images/wind_turbine/wind_small.png" width=800 />
# MAGIC 
# MAGIC *Data Source Acknowledgement: This Data Source Provided By NREL*
# MAGIC 
# MAGIC Gearbox Reliability Collaborative
# MAGIC 
# MAGIC https://openei.org/datasets/dataset/wind-turbine-gearbox-condition-monitoring-vibration-analysis-benchmarking-datasets

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/databricks-datasets-private/ML/wind_turbine/

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/databricks-datasets-private/ML/wind_turbine/", "/mnt/rwe_renewables/demo/csv", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS anton.turbine_healthy;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS anton.turbine_damaged;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS anton.turbine_unified;
# MAGIC 
# MAGIC DROP DATABASE IF EXISTS anton CASCADE;

# COMMAND ----------

from pyspark.sql.functions import exp, lit
gz_input_files = "/mnt/rwe_renewables/demo/csv/"
column_names = ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10"]

damaged_sensor_readings_df = spark.read.option("inferSchema", True).csv(gz_input_files + "/D*gz").drop("_c8", "_c9").toDF(*column_names).withColumn("STATUS", lit("DAMAGED")).cache()
healthy_sensor_readings_df = spark.read.option("inferSchema", True).csv(gz_input_files + "/H*gz").drop("_c8").toDF(*column_names).withColumn("STATUS", lit("HEALTHY")).cache()

# COMMAND ----------

dbutils.fs.rm("/mnt/rwe_renewables/demo/delta", recurse=True)

# COMMAND ----------

union_sensor_readings_df = damaged_sensor_readings_df.union(healthy_sensor_readings_df)

# COMMAND ----------

union_sensor_readings_df.createOrReplaceTempView("union_sensor_readings")

# COMMAND ----------

(union_sensor_readings_df.write
   .format("delta")
   .mode("overwrite")
   .save("/mnt/rwe_renewables/demo/delta/union/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS anton;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS anton.turbine_union (
# MAGIC   AN3 DOUBLE,
# MAGIC   AN4 DOUBLE,
# MAGIC   AN5 DOUBLE,
# MAGIC   AN6 DOUBLE,
# MAGIC   AN7 DOUBLE,
# MAGIC   AN8 DOUBLE,
# MAGIC   AN9 DOUBLE,
# MAGIC   AN10 DOUBLE,
# MAGIC   STATUS STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/rwe_renewables/demo/delta/union/';
# MAGIC OPTIMIZE anton.turbine_union

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM anton.turbine_union ORDER BY RAND()

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC sparkR.session()
# MAGIC results <- sql("SELECT * FROM anton.turbine_union ORDER BY RAND() LIMIT 100000")
# MAGIC rdf <- collect(results)

# COMMAND ----------

# MAGIC %r
# MAGIC library(ggplot2)
# MAGIC rplot <- ggplot(rdf, aes(x=factor(STATUS), y=AN3)) + stat_summary(fun.y="mean", geom="bar")
# MAGIC rplot

# COMMAND ----------

# MAGIC %r
# MAGIC createOrReplaceTempView(results, "results_view")

# COMMAND ----------

pySparkDf = spark.sql("select * from results_view")

# COMMAND ----------

import numpy as np
x = np.array(pySparkDf.select('AN3').collect())
y = np.array(pySparkDf.select('AN10').collect())

# COMMAND ----------

np.shape(x)

# COMMAND ----------

from plotly.offline import iplot
from plotly.graph_objs import Scatter

pandasDf = pySparkDf.toPandas()

# COMMAND ----------

traces = []

for s in ["DAMAGED", "HEALTHY"]:
  data = pandasDf[pandasDf['STATUS'] == s]
  traces += [Histogram(x=data['AN8'], name=s)]

iplot(traces)

# COMMAND ----------

pandasDf.groupby(['STATUS']).mean()

# COMMAND ----------

