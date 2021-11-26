from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


# From a single value line, create a tuple, parsing this to RDD
# makes a key value RDD
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
# With custom function, creates key value RDD
rdd = lines.map(parseLine)
# with mapValues, key is untouched, and transform value data
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
