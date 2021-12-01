from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


# Line: Weather_station_ID, YYYYMMDD, Type, Celsius, ...
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    # Celsius to fahrenheit formula
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)


lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
# Convert 3 length list to key value pair by removing the type in the middle
# (Station ID, Minimum temperature)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# For multiple data in a station ID, find the min value in all the mins
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
