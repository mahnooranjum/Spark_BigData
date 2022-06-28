
import numpy as np
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
from pyspark.sql.functions import when

spark = SparkSession.builder.appName('Basic').getOrCreate()



path = "./outlier_test.csv"
df = spark.read.csv(path)



df.printSchema()


df = df.withColumn("_c0", df["_c0"].cast(IntegerType()))
df = df.withColumn("_c2", df["_c2"].cast(DoubleType()))
df = df.withColumn("_c3", df["_c3"].cast(DoubleType()))


df.printSchema()

df.show()

# df = find_outliers(df)

numeric_columns = [i[0] for i in df.dtypes if i[1]!='string']

for column in numeric_columns:

    # Q1  Q3 Quantile
    Q1 = df.approxQuantile(column,[0.25],relativeError=0)
    Q3 = df.approxQuantile(column,[0.75],relativeError=0)
    
    # get IQR 
    IQR = Q3[0] - Q1[0]
    
    # set limits -1.5*IQR to + 1.5*IQR
    lower =  Q1[0] - 1.5*IQR
    upper =  Q3[0] + 1.5*IQR
    
    colname_outlier = 'is_outlier_{}'.format(column)
    
    df = df.withColumn(colname_outlier, when((df[column] > upper) | (df[column] < lower), 1).otherwise(0))

selected_outliers = [column for column in list(df.columns) if column.startswith("is_outlier_")]

df = df.withColumn('master_outlier', sum(df[column] for column in selected_outliers))
df = df.drop(*[i for i in df.columns if i.startswith("is_outlier")])
df = df.filter(df['master_outlier']<1)
df = df.drop("master_outlier")


df.show()
