from pyspark.sql import SparkSession, functions as F
import os
os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.10"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3.10"


# Initialize Spark Session
spark = SparkSession.builder.appName("HetioNet Analysis").getOrCreate()

# Load nodes and edges TSV files
nodes_df = spark.read.csv("nodes_test.tsv", sep="\t", inferSchema=True, header=True)
edges_df = spark.read.csv("edges_test.tsv", sep="\t", inferSchema=True, header=True)

# Filter edges to keep only the CdG metaedge
cdg_df = edges_df.filter(edges_df.metaedge == "CdG")

# Map each source compound to a tuple (source compound, 1)
mapped_rdd = cdg_df.rdd.map(lambda x: (x["source"], 1))

# Reduce by key (source compound) to count the number of other compounds
reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

# Transform reduced_rdd into a format that can be joined with nodes_df
transformed_rdd = reduced_rdd.map(lambda x: (x[0], x[1]))

# Create a DataFrame from the transformed RDD
reduced_df = spark.createDataFrame(transformed_rdd, ["source", "downregulates_count"])

# Join with nodes to get compound names
cdg_results = reduced_df.join(nodes_df, reduced_df["source"] == nodes_df["id"]) \
                        .select("name", "downregulates_count") \
                        .orderBy(F.desc("downregulates_count")) \
                        .limit(5)

cdg_results.show()
