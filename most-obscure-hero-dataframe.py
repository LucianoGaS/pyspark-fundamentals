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

MostUnpopular = connections.sort(F.col("connections").asc()).first()
MostUnpopularName = names.filter(F.col("id") == MostUnpopular[0]).select("name").first()

print(MostUnpopularName[0] + " is the most popular superhero with "+ str(MostUnpopular[1]) + " co-apperances")

connections2 = (lines
               .withColumn("id", F.split(F.col("value"), " ")[0])
               .withColumn("connections", F.size(F.split(F.col("value"), " ")) -1)
               .groupBy("id").agg(F.sum("connections").alias("connections"))
               #.sort(F.col("connections").asc())
               .filter(F.col('connections') == 1)
               )

UnpopularHeros = connections2.join(names, "id").select("name").show()
