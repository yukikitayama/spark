import re
from pyspark import SparkConf, SparkContext


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalizeWords)

# (x, 1) extra 1 counts up the number of data
# x key is a word, and x + y counts up
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# Sort by the count in an ascending order
# x[1] is count, x[0] is a word
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
