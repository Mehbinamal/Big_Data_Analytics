import findspark
findspark.init("/opt/spark")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, isnull, mean, stddev, min, max, skewness, kurtosis,when
)

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("DataProfiling").getOrCreate()

# Step 2: Load CSV data
df = spark.read.csv("data_profile.csv", header=True, inferSchema=True)

# Step 3: Show schema and preview
print("📌 Schema:")
df.printSchema()

print("\n📌 First 5 rows:")
df.show(5)

# Step 4: Column count and data types
print("\n📌 Column Info:")
for field in df.schema.fields:
    print(f"- {field.name}: {field.dataType}")

# Step 5: Null count per column
print("\n📌 Null Value Count:")
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Step 6: Distinct value count per column
print("\n📌 Distinct Values per Column:")
df.select([countDistinct(col(c)).alias(c) for c in df.columns]).show()

# Step 7: Basic statistics for numeric columns
numeric_cols = [field.name for field in df.schema.fields if str(field.dataType) in ['IntegerType', 'DoubleType', 'LongType']]
print("\n📌 Summary Stats:")
df.select(numeric_cols).describe().show()

# Step 8: Skewness and Kurtosis
print("\n📌 Skewness and Kurtosis:")
for col_name in numeric_cols:
    df.select(
        skewness(col(col_name)).alias(f"{col_name}_skewness"),
        kurtosis(col(col_name)).alias(f"{col_name}_kurtosis")
    ).show()

# Step 9: Correlation (only between numeric columns)
print("\n📌 Correlation Matrix:")
for i in range(len(numeric_cols)):
    for j in range(i + 1, len(numeric_cols)):
        col1, col2 = numeric_cols[i], numeric_cols[j]
        corr_value = df.stat.corr(col1, col2)
        print(f"Correlation between {col1} and {col2}: {corr_value:.4f}")

# Stop Spark session
spark.stop()
