from pyspark import SparkConf, SparkContext # two libraries that can only be used together; Conf is the conficuration for SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("CustomerSpent") #setMaster("local") means the code will run in my machine not a cluster
sc = SparkContext(conf = conf) # standard SparkContext object name sc

def parseline(line):
    fields = line.split(',')
    CustomerID = int(fields[0])
    Price = float(fields[2])
    
    return (CustomerID, Price)


lines = sc.textFile('customer-orders.csv')
rdd = lines.map(parseline)
TotalSpent = rdd.reduceByKey(lambda x, y: x + y) # x is a tuple; second function x is a key y is the Price
SortedCustomers = TotalSpent.map(lambda xy: (xy[1], xy[0])).sortByKey() # sort by who spent more
results = SortedCustomers.collect()

for amount, C_id in results:
        print(str(C_id)+":\t"+ str(amount))
 
from pyspark.sql import SparkSession
# Must set this env variable to avoid warnings
import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
import pyspark.sql.types as T
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName('CustomerSpent')
    .config("spark.sql.ansi.enabled", "false")
    .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1")
    .getOrCreate() # application name/project or model that you've been work on
)

schema = T.StructType([ #define the db columns and datatype equal sql
        T.StructField('Customer_ID', T.IntegerType(), True),
        T.StructField('Item_ID', T.IntegerType(), True),
        T.StructField('Price', T.FloatType(), True)
])

df = spark.read.schema(schema).csv('customer-orders.csv')
#df = df.toDF("Customer_ID", "Item_ID", "Price")

#df.printSchema()
#df.show()

(df.groupBy('Customer_ID').agg(F.round(F.sum('Price'), 2)
    .alias("Total_Spent"))
    .orderBy('Total_Spent')
    .show()
 )

spark.stop()
