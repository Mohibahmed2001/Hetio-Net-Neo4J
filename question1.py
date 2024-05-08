from pyspark.sql import SparkSession, functions as F

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("TSV Processing") \
    .getOrCreate()

# Read the nodes TSV
nodes_df = spark.read.csv(
    "nodes_test.tsv",
    sep="\t",
    inferSchema=True,
    header=True
)

# Read the edges TSV
edges_df = spark.read.csv(
    "edges_test.tsv",
    sep="\t",
    inferSchema=True,
    header=True
)

# Show the first few rows of each DataFrame to verify everything is loaded correctly
nodes_df.show()
edges_df.show()
