from pyspark.sql import SparkSession, functions as F

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("TSV Processing") \
    .getOrCreate()

# Read the nodes_edges.tsv
nodes_df = spark.read.csv(
    "nodes_test.tsv",
    sep="\t",
    inferSchema=True,
    header=True
)

# Read the edges_test.tsv
edges_df = spark.read.csv(
    "edges_test.tsv",
    sep="\t",
    inferSchema=True,
    header=True
)

# Show the first few rows 
nodes_df.show()
edges_df.show()
