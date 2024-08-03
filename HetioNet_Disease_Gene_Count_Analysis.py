from pyspark.sql import SparkSession, functions as F
import os
os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.10"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3.10"


# Initialize Spark Session
spark = SparkSession.builder.appName("HetioNet Analysis").getOrCreate()

# Load nodes and edges TSV files
nodes_df = spark.read.csv("nodes_test.tsv", sep="\t", inferSchema=True, header=True)
edges_df = spark.read.csv("edges_test.tsv", sep="\t", inferSchema=True, header=True)

# Filter edges to keep only the DuG metaedge
dug_df = edges_df.filter(edges_df.metaedge == "DuG")

# Map each source disease to a tuple (source disease, 1)
mapped_rdd = dug_df.rdd.map(lambda x: (x["source"], 1))

# Reduce by key (source disease) to count the number of other compounds
reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

# Transform reduced_rdd into a format that can be joined with nodes_df
transformed_rdd = reduced_rdd.map(lambda x: (x[0], x[1]))

# Create a DataFrame from the transformed RDD
reduced_df = spark.createDataFrame(transformed_rdd, ["source", "gene_count"])

# Join with nodes to get disease names
dug_results = reduced_df.join(nodes_df, reduced_df["source"] == nodes_df["id"]) \
                        .select("name", "gene_count") \
                        .orderBy(F.desc("gene_count")) \
                        .limit(5)

dug_results.show()
