from pyspark.sql import SparkSession
import os
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\ericm\\AppData\\Local\\Programs\\Python\\Python310\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\ericm\\AppData\\Local\\Programs\\Python\\Python310\\python.exe"

spark = SparkSession.builder.appName("HetioNet Analysis").getOrCreate()

# Load edges_test.tsv
edges_df = spark.read.option("delimiter", "\t").csv("edges_test.tsv").toDF("source", "metaedge", "target")

# We only keep edges with a meta edge of "CbG"
cbg_df = edges_df.filter(edges_df.metaedge == "CbG")

# Map each compound 
mapped_rdd = cbg_df.rdd.map(lambda x: (x[0], x[2]))

# Reduce by key to count the number of genes bound to each compound
reduced_rdd = mapped_rdd.groupByKey().mapValues(len)

# Sort the results by in descending order
sorted_rdd = reduced_rdd.sortBy(lambda x: x[1], ascending=False)

# Pick top 5 including gene count
top_5 = sorted_rdd.take(5)

# Create a DF of top 5 commpounds
top_5_df = spark.createDataFrame(top_5, ["Compound", "Gene Count"])
top_5_df.show()
spark.stop()
