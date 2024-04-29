from pyspark.sql import SparkSession, functions as F
# Initialize Spark Session
spark = SparkSession.builder.appName("HetioNet Analysis").getOrCreate()

# Load nodes and edges TSV files
nodes_df = spark.read.csv("nodes_test.tsv", sep="\t", inferSchema=True, header=True)
edges_df = spark.read.csv("edges_test.tsv", sep="\t", inferSchema=True, header=True)

cdg_counts = edges_df.filter(edges_df.metaedge == "CdG") \
                     .groupBy("source").count() \
                     .withColumnRenamed("count", "compound_count")

# Join with nodes to get compound names
cdg_results = cdg_counts.join(nodes_df, cdg_counts.source == nodes_df.id) \
                        .select("name", "compound_count") \
                        .orderBy(F.desc("compound_count")) \
                        .limit(5)

cdg_results.show()