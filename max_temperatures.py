from pyspark import SparkConf, SparkContext
from typing import Tuple


conf = SparkConf().setMaster('local').setAppName('MaxTemperatures')
sc = SparkContext(conf=conf)


def parse_line(line: str) -> Tuple[str, str, float]:
    fields = line.split(',')
    station_id = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3]) * 0.1 * (9 / 5) + 32
    return station_id, entry_type, temperature


lines = sc.textFile('file:///SparkCourse/1800.csv')
parsed_lines = lines.map(parse_line)
max_temps = parsed_lines.filter(lambda x: 'TMAX' in x[1])
station_temps = max_temps.map(lambda x: (x[0], x[2]))
max_temps = station_temps.reduceByKey(lambda x, y: max(x, y))
results = max_temps.collect()

for result in results:
    print(f'{result[0]}\t{result[1]:.2f}F')
