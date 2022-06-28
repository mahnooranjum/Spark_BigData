
import numpy as np
from sympy import I
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField, 
                               StringType,
                               IntegerType,
                               FloatType, 
                               DoubleType,
                               DecimalType,
                               BooleanType,
                               StructType)


spark = SparkSession.builder.appName('Basic').getOrCreate()

path = "./fakefriends_nulls.csv"
df = spark.read.csv(path)


df.printSchema()


# df = df.withColumn("_c0", df("_c0").cast(DateType)).drop("_c0")

df = df.withColumn("_c0", df["_c0"].cast(IntegerType()))
df = df.withColumn("_c2", df["_c2"].cast(DoubleType()))
df = df.withColumn("_c3", df["_c3"].cast(DoubleType()))
df.printSchema()


numeric_columns = [i[0] for i in df.dtypes if i[1]!='string']


print(numeric_columns)



print(df.head())