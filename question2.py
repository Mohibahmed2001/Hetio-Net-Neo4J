import os
from pyspark.sql import SparkSession, functions as F

# Set environment variable for Hadoop Home to suppress warnings on Windows
os.environ['HADOOP_HOME'] = 'C:\\hadoop'

# Initialize Spark Session
spark = SparkSession.builder.appName("HetioNet Analysis").getOrCreate()

# Load nodes and edges TSV files
nodes_df = spark.read.csv("nodes_test.tsv", sep="\t", inferSchema=True, header=True)
edges_df = spark.read.csv("edges_test.tsv", sep="\t", inferSchema=True, header=True)

# Task 2: Counting Gene Bindings to Compounds (CbG)
cbg_counts = edges_df.filter(edges_df.metaedge == "CbG") \
                     .groupBy("source").count() \
                     .withColumnRenamed("count", "gene_count")

# Join with nodes to get compound names
cbg_results = cbg_counts.join(nodes_df, cbg_counts.source == nodes_df.id) \
                        .select(nodes_df.name, "gene_count") \
                        .orderBy(F.desc("gene_count")) \
                        .limit(5)

# Display the top 5 compounds with their gene counts
cbg_results.show()

# Ensure to stop the Spark session at the end of the script to free up resources
spark.stop()