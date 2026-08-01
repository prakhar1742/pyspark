from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType,StructField, DateType
from pyspark.sql.functions import col, lower, row_number, desc,concat, current_date, datediff, year
from pyspark.sql.window import Window
import math

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
employee_df.cache()
employee_df.printSchema()
total_employees=employee_df.count()

employee_df.select("first_name","last_name")

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

employee_with_dept_info= employee_with_dept_info.withColumnsRenamed({"emp_no":"employee_id","birth_date":"dob"})

with_no_gender=employee_with_dept_info.drop(employee_with_dept_info["gender"])

employee_with_dept_info=employee_with_dept_info.dropDuplicates()

department_df.distinct().show()


employee_with_dept_info=employee_with_dept_info.sort(employee_with_dept_info.first_name.asc(),employee_with_dept_info.hire_date.asc())

male_employees= employee_with_dept_info.filter(col("gender")=="M")
male_employees.show()

hired_after_1996 = employee_with_dept_info.filter(col("hire_date")> "1995-01-01").show()

born_before_1960 = employee_with_dept_info.filter(col("dob")<"1960-01-01").show()
employee_john = employee_with_dept_info.filter(lower(col("first_name"))=="john")

name_starts_with_A= employee_with_dept_info.filter(lower(col("first_name")).startswith("a")).show()

salary_schema=StructType([
    StructField("emp_no",IntegerType(),False),
    StructField("salary",IntegerType(),False),
    StructField("from_date",DateType(),False),
    StructField("to_date",DateType(),False)
])

salary_df=spark.read.option("header",True).schema(salary_schema).csv("C:/Users/Prakhar/Desktop/Pysrapk/joins/salaries.csv")


window = Window.partitionBy("emp_no").orderBy(desc(col("from_date")))
latest_salary = salary_df.withColumn("rn",row_number().over(window)).filter(col("rn")==1).drop("rn")
emp_dept_sal= employee_with_dept_info.join(
    latest_salary,
    on=latest_salary["emp_no"]==employee_with_dept_info["employee_id"],
    how="inner"  
)
emp_with_80000_saalary = emp_dept_sal.filter(col("salary")>80000).show()

emp_dept_sal=emp_dept_sal.withColumn("full_name",concat(col("first_name"),col("last_name")))

emp_dept_sal.show()

emp_dept_sal=emp_dept_sal.withColumn("age",datediff(current_date(),col("dob")))

emp_dept_sal=emp_dept_sal.withColumn("years_worked",math.floor(datediff(col("to_date"),col("from_date")))/365)

emp_dept_sal=emp_dept_sal.withColumn("hire_year",year(col("from_date")))




emp_dept_sal.show()