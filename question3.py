from pyspark.sql import SparkSession, functions as F
# Initialize Spark Session
spark = SparkSession.builder.appName("HetioNet Analysis").getOrCreate()

# Load nodes and edges TSV files
nodes_df = spark.read.csv("nodes_test.tsv", sep="\t", inferSchema=True, header=True)
edges_df = spark.read.csv("edges_test.tsv", sep="\t", inferSchema=True, header=True)

dug_counts = edges_df.filter(edges_df.metaedge == "DuG") \
                     .groupBy("source").count() \
                     .withColumnRenamed("count", "gene_count")

# Join with nodes to get disease names
dug_results = dug_counts.join(nodes_df, dug_counts.source == nodes_df.id) \
                        .select("name", "gene_count") \
                        .orderBy(F.desc("gene_count")) \
                        .limit(5)

dug_results.show()