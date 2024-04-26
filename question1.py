from pyspark.sql import SparkSession

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

# Example operation: Join nodes based on an edge
# Assuming you want to join nodes based on the source-target relationship in edges
joined_df = edges_df.join(
    nodes_df,
    edges_df.source == nodes_df.id,
    "inner"
).select("source", "name", "target")

# Show the result of the join
joined_df.show()
