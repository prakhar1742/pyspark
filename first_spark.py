from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from pyspark.sql.functions import col, lower

# ---------------------------------------------------------
# Create Spark Session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Employee CSV Analysis")
    .master("local[2]")
    .getOrCreate()
)

# ---------------------------------------------------------
# Define Schema
# ---------------------------------------------------------

schema = StructType([
    StructField("Education", StringType(), False),
    StructField("JoiningYear", IntegerType(), False),
    StructField("City", StringType(), False),
    StructField("PaymentTier", IntegerType(), False),
    StructField("Age", IntegerType(), False),
    StructField("Gender", StringType(), False),
    StructField("EverBenched", StringType(), False),
    StructField("ExperienceInCurrentDomain", IntegerType(), False),
    StructField("LeaveOrNot", IntegerType(), False)
])

# ---------------------------------------------------------
# Read CSV
# ---------------------------------------------------------

df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv("C:/Users/Prakhar/Downloads/Employee.csv")
)

# ---------------------------------------------------------
# Dataset Overview
# ---------------------------------------------------------

df.printSchema()
df.show(10)

# ---------------------------------------------------------
# Basic Information
# ---------------------------------------------------------

total_rows = df.count()
total_columns = len(df.columns)
column_names = df.columns

# ---------------------------------------------------------
# Column Selection
# ---------------------------------------------------------

education_age_df = df.select("Education", "Age")
education_age_df.show()

# ---------------------------------------------------------
# Filtering
# ---------------------------------------------------------

employees_above_30 = df.filter(col("Age") > 30)

employees_age_25 = df.filter(col("Age") == 25)

employees_bangalore = df.filter(
    lower(col("City")) == "bangalore"
)

# ---------------------------------------------------------
# Distinct Values
# ---------------------------------------------------------

unique_cities = df.select("City").distinct()

number_of_unique_cities = (
    df.select("City")
    .distinct()
    .count()
)

# ---------------------------------------------------------
# Display Results
# ---------------------------------------------------------

employees_above_30.show()

employees_age_25.show()

employees_bangalore.show()

unique_cities.show()

print(f"Total Rows            : {total_rows}")
print(f"Total Columns         : {total_columns}")
print(f"Unique Cities         : {number_of_unique_cities}")
print(f"Column Names          : {column_names}")