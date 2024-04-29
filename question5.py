import os
import sys
from pyspark.sql import SparkSession, functions as F

# Environment setup for PySpark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ['HADOOP_HOME'] = 'C:\\hadoop'

# Try setting a temporary directory that might have better handling
temp_dir = "C:\\path_to_better_temp_dir"  # Ensure this path is correct and writable
os.environ["SPARK_LOCAL_DIRS"] = temp_dir  # Setting Spark local directory for temporary files

# Initialize Spark Session with a configuration that explicitly sets the directory for temporary files
spark = SparkSession.builder \
    .appName("HetioNet Analysis") \
    .config("spark.local.dir", temp_dir) \
    .getOrCreate()

# Load nodes and edges TSV files
nodes_df = spark.read.csv("nodes_test.tsv", sep="\t", inferSchema=True, header=True)
edges_df = spark.read.csv("edges_test.tsv", sep="\t", inferSchema=True, header=True)

# Function to extract IDs from DataFrame results
def get_ids(df, column):
    return df.select(column).rdd.flatMap(lambda x: x).collect()

# Collecting data for hash experiments
cbg_results = edges_df.filter(edges_df.metaedge == "CbG").groupBy("source").count().orderBy(F.desc("count")).limit(5)
dug_results = edges_df.filter(edges_df.metaedge == "DuG").groupBy("source").count().orderBy(F.desc("count")).limit(5)
cdg_results = edges_df.filter(edges_df.metaedge == "CdG").groupBy("source").count().orderBy(F.desc("count")).limit(5)

# Collecting results
cbg_ids = get_ids(cbg_results, "source")
dug_ids = get_ids(dug_results, "source")
cdg_ids = get_ids(cdg_results, "source")
all_ids = cbg_ids + dug_ids + cdg_ids  # Combine IDs for hashing

# Define hash functions
def mid_square_hash(value, r):
    value_str = ''.join(str(ord(char)) for char in value)
    squared_value = str(int(value_str) ** 2)
    start_index = len(squared_value) // 2 - r // 2
    end_index = start_index + r
    return int(squared_value[start_index:end_index]) % 10 if start_index < end_index else 0

def folding_hash(value, digit_size):
    value_str = ''.join(str(ord(char)) for char in value)
    parts = [value_str[i:i+digit_size] for i in range(0, len(value_str), digit_size)]
    return sum(int(part) for part in parts if part) % (10 ** digit_size) % 10

# Create hash tables using the defined hash functions
def create_hash_table(data, hash_function):
    hash_table = [list() for _ in range(10)]  # 10 buckets
    for id in data:
        bucket = hash_function(id, 3 if hash_function == mid_square_hash else 2)  # r=3 for mid_square, digit_size=2 for folding
        hash_table[bucket].append(id)
    return hash_table

# Hash tables using mid-square and folding methods
mid_square_table = create_hash_table(all_ids, mid_square_hash)
folding_table = create_hash_table(all_ids, folding_hash)

# Calculate and print the size of hash tables
print("Size of Mid-Square Hash Table:", sys.getsizeof(mid_square_table))
print("Size of Folding Hash Table:", sys.getsizeof(folding_table))

# Stop the Spark session
spark.stop()