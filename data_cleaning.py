from pyspark.sql.functions import col, trim, when


def clean_data(df):
    """
    Cleans the input PySpark DataFrame by:
    1. Dropping duplicate rows
    2. Removing rows with nulls in critical columns
    3. Trimming whitespace from string columns
    4. Converting column data types (example included)
    5. Filtering out invalid values (example included)

    Parameters:
        df (pyspark.sql.DataFrame): The input DataFrame

    Returns:
        pyspark.sql.DataFrame: The cleaned DataFrame
    """
    # Step 1: Drop duplicates
    df = df.dropDuplicates()

    # Step 2: Drop rows with nulls in critical columns (modify as needed)
    df = df.dropna(subset=["name", "age"])  # example critical columns

    # Step 3: Trim whitespace from all string columns
    string_cols = [field.name for field in df.schema.fields if field.dataType.simpleString() == 'string']
    for col_name in string_cols:
        df = df.withColumn(col_name, trim(col(col_name)))

    # Step 4: Convert 'age' column to IntegerType (example)
    df = df.withColumn("age", col("age").cast("int"))

    # Step 5: Filter out invalid ages (e.g., age < 0 or null)
    df = df.filter((col("age") >= 0) & (col("age").isNotNull()))

    return df

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row

# Initialize Spark session (if not already created)
spark = SparkSession.builder.appName("DataCleaningExample").getOrCreate()

# Sample data with duplicates, nulls, whitespace, and invalid ages
data = [
    Row(name=" Alice ", age="25"),
    Row(name="Bob", age="30"),
    Row(name=" Alice ", age="25"),  # duplicate
    Row(name="Charlie", age=None),  # null age
    Row(name=None, age="22"),  # null name
    Row(name="David", age="-5"),  # invalid age
    Row(name="Eve", age=" 40 "),
]

# Define schema (optional, but recommended for clarity)
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", StringType(), True),
])

# Create DataFrame
raw_df = spark.createDataFrame(data, schema)

# Show raw data
raw_df.show()

cleaned_df = clean_data(raw_df)
cleaned_df.show()
