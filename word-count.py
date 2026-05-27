from pyspark import SparkConf, SparkContext # two libraries that can only be used together; Conf is the conficuration for SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount") #setMaster("local") means the code will run in my machine not a cluster
sc = SparkContext(conf = conf) # standard SparkContext object name sc

def normalizeWords(text): #2 and 3 method
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile('Book')
#words = input.flatMap(lambda x: x.split()) # 1 method
words = input.flatMap(normalizeWords)# improvement - #2 and 3 methods
#wordCounts = words.countByValue()
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda xy: (xy[1], xy[0])).sortByKey()
results = wordCountsSorted.collect()
 
#for word, count in wordCounts.items(): #1 and 2 methods
    #cleanWord = word.encode('ascii', 'ignore')
    #if (cleanWord):
        #print(cleanWord, count)

for result in results: #3 method
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode("utf-8") +":\t\t"+count)

 
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName('WordCount')
    .config('spark.sql.repl.eagerEval.enabled', True) # read tables equal sql; if false he just shiw the header and typer of each column
    .getOrCreate() # application name/project or model that you've been work on
)

df = spark.read.text('Book')

(df.select(
    #F.explode(F.split(df.value, " ")) #1 method
    F.explode(F.split(F.lower(df.value), r"\W+")) # 2 and 3 method
    .alias("word"))
    .filter(F.col('word') != "") # exclude empty strings
    .groupBy("word").count()
    .orderBy("count", ascending=False) # method 3
    .show()
 )

spark.stop()
