import pyspark
import random

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CalculatePi") \
    .get_session()

# Number of samples
partitions = 2
n = 100000 * partitions

def f(_):
    x = random.random() * 2 - 1
    y = random.random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0

# Parallelize the task and count points inside the circle
count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(lambda add, b: add + b)

print(f"Pi is roughly {4.0 * count / n}")

spark.stop()