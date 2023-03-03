from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
from pyspark.sql.functions import *

Bank_DF = spark.read.format("csv").load("/mnt/c/Users/vini/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv", header=True, sep = ";", escape = ",", inferSchema = True )

Bank_DF.registerTempTable("Bank")

output=spark.sql("select (case when age<13 then 'Kids' when age<20 then 'Teenagers' \
           when age < 31 then 'Young' \
           when age<50 then 'MiddleAgers' else 'Seniors' end) as peopletype,count(age) from bank where y='yes' group by peopletype order by count(age)")

output.write.parquet("hdfs://localhost:9000/user/training/bankmarketing/out/parquet10")

data=spark.read.parquet("hdfs://localhost:9000/user/training/bankmarketing/out/parquet10")

data.show()

output1=spark.sql("select (case when age<13 then 'Kids' when age<20 then 'Teenagers' \
           when age < 31 then 'Young' \
           when age<50 then 'MiddleAgers' else 'Seniors' end) as peopletype,count(age) as age from bank where y='yes' group by peopletype having count(age)>2000")

output1.select("peopletype","age").write.format("avro").save("hdfs://localhost:9000/user/training/bankmarketing/out/avro4")

data1=spark.read.format("avro").load("hdfs://localhost:9000/user/training/bankmarketing/out/avro4")

data1.show()