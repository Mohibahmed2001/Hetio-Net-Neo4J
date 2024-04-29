import os
import sys
from pyspark.sql import SparkSession, functions as F

# Setup environment for PySpark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ['HADOOP_HOME'] = 'C:\\hadoop'

# Initialize Spark Session with specific temp directory settings
temp_dir = "C:\\path_to_better_temp_dir"
spark = SparkSession.builder \
    .appName("HetioNet Analysis") \
    .config("spark.local.dir", temp_dir) \
    .getOrCreate()

# Load data
nodes_df = spark.read.csv("nodes_test.tsv", sep="\t", inferSchema=True, header=True)
edges_df = spark.read.csv("edges_test.tsv", sep="\t", inferSchema=True, header=True)

# Function to extract IDs
def get_ids(df, column):
    return df.select(column).rdd.flatMap(lambda x: x).collect()

# Collect data for hash experiments
cbg_ids = get_ids(edges_df.filter(edges_df.metaedge == "CbG").groupBy("source").count().orderBy(F.desc("count")).limit(5), "source")
dug_ids = get_ids(edges_df.filter(edges_df.metaedge == "DuG").groupBy("source").count().orderBy(F.desc("count")).limit(5), "source")
cdg_ids = get_ids(edges_df.filter(edges_df.metaedge == "CdG").groupBy("source").count().orderBy(F.desc("count")).limit(5), "source")
all_ids = cbg_ids + dug_ids + cdg_ids  # Combine IDs for hashing

# Hash functions
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

# Function to create and measure hash tables
def create_and_measure_hash_tables(data, hash_func, parameter_range):
    sizes = {}
    for param in parameter_range:
        hash_table = [list() for _ in range(10)]  # 10 buckets
        for id in data:
            bucket = hash_func(id, param) % 10
            hash_table[bucket].append(id)
        sizes[param] = sys.getsizeof(hash_table) + sum(sys.getsizeof(bucket) for bucket in hash_table)
    return sizes

# Run experiments
mid_square_sizes = create_and_measure_hash_tables(all_ids, mid_square_hash, [3, 4])
folding_sizes = create_and_measure_hash_tables(all_ids, folding_hash, [2, 3])

print("Mid-Square Hash Table Sizes:", mid_square_sizes)
print("Folding Hash Table Sizes:", folding_sizes)

# Clean up Spark session
spark.stop()