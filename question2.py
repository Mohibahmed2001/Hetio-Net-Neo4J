from pyspark.sql import SparkSession
import os
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\ericm\\AppData\\Local\\Programs\\Python\\Python310\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\ericm\\AppData\\Local\\Programs\\Python\\Python310\\python.exe"

spark = SparkSession.builder.appName("HetioNet Analysis").getOrCreate()

# Load edges TSV file
edges_df = spark.read.option("delimiter", "\t").csv("edges_test.tsv").toDF("source", "metaedge", "target")

# Filter to keep only the edges where metaedge is "CbG"
cbg_df = edges_df.filter(edges_df.metaedge == "CbG")

# Map each compound to a tuple (compound_id, gene_id)
mapped_rdd = cbg_df.rdd.map(lambda x: (x[0], x[2]))

# Reduce by key (compound_id) to count the number of genes bound to each compound
reduced_rdd = mapped_rdd.groupByKey().mapValues(len)

# Sort the results by the count of genes in descending order
sorted_rdd = reduced_rdd.sortBy(lambda x: x[1], ascending=False)

# Take the top 5 compounds with their corresponding gene counts
top_5 = sorted_rdd.take(5)

# Create a DataFrame from the top 5 compounds and gene counts
top_5_df = spark.createDataFrame(top_5, ["Compound", "Gene Count"])

# Show the DataFrame
top_5_df.show()

# Ensure to stop the Spark session at the end of the script to free up resources
spark.stop()
