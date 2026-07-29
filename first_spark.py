from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col,lower

spark=SparkSession.builder.appName("employee_csv").master("local[2]").getOrCreate()

schema = StructType([
    StructField("Education",StringType(),False),
    StructField("JoiningYear",IntegerType(),False),
    StructField("City",StringType(),False),
    StructField("PaymentTier",IntegerType(),False),
    StructField("Age",IntegerType(),False),
    StructField("Gender",StringType(),False),
    StructField("EverBenched",StringType(),False),
    StructField("ExperienceInCurrentDomain",IntegerType(),False),
    StructField("LeaveOrNot",IntegerType(),False)
])
df=spark.read.option("header",True).schema(schema).csv("C:/Users/Prakhar/Downloads/Employee.csv")

df.printSchema()
print("this is schema\n\n\n")

print(df.columns)
df1=df.select("Education","Age")
total=df.count()
print(total," rows in the dataset")
df.show(10)
df1.show()


print("For total columns")

print(len(df.columns))
print("================")

for column in df.columns:
    print(column)

only_30 = df.filter(col("Age")>30)
only_30.show()


exact_25= df.filter(col("Age")==25)
exact_25.show()

from_bangalore=df.filter(lower(col("City"))=="Bangalore")
from_bangalore.show()

unique_cities=df.select("City").distinct()

unique_cities.show()

unique_cities_count=df.select("City").groupBy("City").count()
unique_cities_count.show()
