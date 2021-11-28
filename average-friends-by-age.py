from pyspark.sql import SparkSession
from pyspark.sql import functions as func


spark = SparkSession.builder.appName('AverageFriendsByAge').getOrCreate()

people = spark.read\
    .option('header', 'true')\
    .option('inferSchema', 'true')\
    .csv('file:///SparkCourse/fakefriends-header.csv')

people.printSchema()

friends_by_age = people.select('age', 'friends')

print('Average number of friends by age')
# friends_by_age.groupBy('age').avg('friends').sort('age').show()
friends_by_age.groupBy('age').agg(func.round(func.avg('friends'), 2).alias('friends_avg')).sort('age').show()

spark.stop()
