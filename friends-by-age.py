#from pyspark import SparkConf, SparkContext # two libraries that can only be used together; Conf is the conficuration for SparkContext

#conf = SparkConf().setMaster("local").setAppName("FriendsByAge") #setMaster("local") means the code will run in my machine not a cluster
#sc = SparkContext(conf = conf) # standard SparkContext object name sc

#def parseline(line):
    #fields = line.split(',')
    #age = int(fields[2])
    #numFriends = int(fields[3])
    #return (age, numFriends)

#lines = sc.textFile('fakefriends.csv')
#rdd = lines.map(parseline)
#totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) # mapValues altera o valor dos dados
#averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
#results = averagesByAge.collect()
#for result in results:
    #print(result)
    
    
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName('FriendsByAge')
    .config('spark.sql.repl.eagerEval.enabled', True) # read tables equal sql; if false he just shiw the header and typer of each column
    .getOrCreate() # application name/project or model that you've been work on
)

df = spark.read.csv('fakefriends.csv')

#print(df.describe())
(
    df
    .groupBy('_c2')
    .agg(F.mean('_c3').alias('qnt'))
    .show()
)

spark.stop()
