from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType,StructField, DateType

spark = SparkSession.builder.appName("joins").master("local[2]").getOrCreate()

schema = StructType([
    StructField("emp_no", IntegerType(), False),
    StructField("birth_date", DateType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("hire_date", DateType(), False),
])
employee_df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv("C:/Users/Prakhar/Desktop/Pysrapk/joins/employees.csv")
)

department_emp_schema=StructType([
    StructField("emp_no",IntegerType(),False),
    StructField("dept_no",StringType(),False),
    StructField("from_date",DateType(),False),
    StructField("to_date",DateType(),False)
])

department_emp_df=spark.read.option("header",True).schema(department_emp_schema).csv("C:/Users/Prakhar/Desktop/Pysrapk/joins/dept_emp.csv")

employee_with_department= employee_df.join(
    department_emp_df,
    on=employee_df["emp_no"]==department_emp_df["emp_no"],
    how="inner"
).drop(department_emp_df["emp_no"])

department_schema = StructType([
    StructField("dept_no", StringType(), False),
    StructField("dept_name", StringType(), False),
])

department_df=spark.read.option("header",True).schema(department_schema).csv("C:/Users/Prakhar/Desktop/Pysrapk/joins/departments.csv")


employee_with_dept_info = employee_with_department.join(
    department_df,
    on=department_df["dept_no"]==employee_with_department["dept_no"],
    how="inner"
).drop(department_df["dept_no"])

employee_with_dept_info.show()


employee_with_dept_info.write.parquet("employee_info.parquet")