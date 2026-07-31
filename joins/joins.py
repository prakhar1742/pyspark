from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType,StructField

spark = SparkSession.builder.appName("joins").master("local[2]").getOrCreate()

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
employee_df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv("C:/Users/Prakhar/Desktop/Pysrapk/Employee.csv")
)

department_schema = StructType([
    StructField("ID", IntegerType(), False),
    StructField("Dept_name", StringType(), False),
    StructField("location", StringType(), False),
    StructField("travel_required", StringType(), False),
])

department_df=spark.read.option("header",True).schema(department_schema).csv("C:/Users/Prakhar/Desktop/Pysrapk/Department_Dataset.csv")

department_df.show()

employee_df.show()