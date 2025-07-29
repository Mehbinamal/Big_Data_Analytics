import findspark
findspark.init("/opt/spark")

from pyspark.sql import SparkSession
from pyspark.sql.functions import log1p, sqrt, col, when
from pyspark.sql.types import DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("PowerTransform").getOrCreate()

df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Select a numeric column to transform (replace with your actual column name)
target_column = "skewed_feature"

# Cast to double (important for log/sqrt to work)
df = df.withColumn(target_column, col(target_column).cast(DoubleType()))

df_transformed = (
    df.withColumn("log_transformed", log1p(col(target_column)))  # log(x + 1)
      .withColumn("sqrt_transformed", sqrt(col(target_column)))  # sqrt(x)
      .withColumn("cuberoot_transformed", col(target_column) ** (1.0 / 3))  # x^(1/3)
)

# Step 5: Show the results
df_transformed.select("id", target_column, "log_transformed", "sqrt_transformed", "cuberoot_transformed").show(truncate=False)

# Optional: Stop Spark session (good practice)
spark.stop()