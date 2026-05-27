from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()


# Create schema when reading u.data
schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

# Load the data as dataframe
names = spark.read.option("sep", " ").schema(schema).csv("Marvel+Names")
lines = spark.read.text("Marvel+Graph")

connections = (lines
               .withColumn("id", F.split(F.col("value"), " ")[0])
               .withColumn("connections", F.size(F.split(F.col("value"), " ")) -1)
               .groupBy("id").agg(F.sum("connections").alias("connections"))
               )

MostPopular = connections.sort(F.col("connections").desc()).first()
MostPopularName = names.filter(F.col("id") == MostPopular[0]).select("name").first()

print(MostPopularName[0] + " is the most popular superhero with "+ str(MostPopular[1]) + " co-apperances")
