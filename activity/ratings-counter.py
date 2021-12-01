from pyspark import SparkConf, SparkContext
import collections

# local means running on a single thread and process, not doing distributed process
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

# Create RDD
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

# x is a line in the text file, and the third item is movie rating
# Each line is 'User_ID Movie_ID Rating Timestamp'
# .map does not transform in place, instead making a new RDD called ratings
ratings = lines.map(lambda x: x.split()[2])

# Because the below method is an Action, it returns Python object
# It's no longer RDD
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
