from pyspark import SparkConf, SparkContext # two libraries that can only be used together; Conf is the conficuration for SparkContext

conf = SparkConf().setMaster("local").setAppName("MinMaxTemp") #setMaster("local") means the code will run in my machine not a cluster
sc = SparkContext(conf = conf) # standard SparkContext object name sc

def parseline(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])*0.1
    
    return (stationID, entryType, temperature)

lines = sc.textFile('1800.csv')
rdd = lines.map(parseline)
minTemps = rdd.filter(lambda x: "TMIN" in x[1])    
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect()

print("Min Temp\n")
for result in results:
    print(result[0]+"\t"+str(result[1])+"°C")

maxTemps = rdd.filter(lambda x: "TMAX" in x[1])    
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = maxTemps.collect()

print("Max Temp\n")
for result in results:
    print(result[0]+"\t"+str(result[1])+"°C")

    
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName('MinMaxTemp')
    .config('spark.sql.repl.eagerEval.enabled', True) # read tables equal sql; if false he just shiw the header and typer of each column
    .getOrCreate() # application name/project or model that you've been work on
)

df = spark.read.csv('1800.csv')

#print(df.describe())
(
    df
    .withColumn('temp', F.col('_c3')*0.1)
    .where(F.col('_c2') == 'TMIN')
    .groupBy('_c0')
    .agg(F.min('temp').alias('min'))
    .show()
)

(
    df
    .withColumn('temp', F.col('_c3')*0.1)
    .where(F.col('_c2') == 'TMAX')
    .groupBy('_c0')
    .agg(F.max('temp').alias('max'))
    .show()
)

spark.stop()
