from pyspark import SparkConf, SparkContext
from typing import Tuple


def parse_line(line: str) -> Tuple[int, int, float]:
    fields = line.split(',')
    customer_id = int(fields[0])
    item_id = int(fields[1])
    amount = float(fields[2])
    return customer_id, item_id, amount


conf = SparkConf().setMaster('local').setAppName('TotalAmountSpentByCustomer')
sc = SparkContext(conf=conf)

# Split
lines = sc.textFile('file:///SparkCourse/customer-orders.csv')
parsed_lines = lines.map(parse_line)

# Map line to key value pairs of customer ID and dollar amount
id_to_amount = parsed_lines.map(lambda x: (x[0], x[2]))

# reduceByKey to get total by customer ID
total_amount = id_to_amount.reduceByKey(lambda x, y: x + y)

# Sort by total amount
sorted_total_amount = total_amount.map(lambda x: (x[1], x[0])).sortByKey()

# collect()
results = sorted_total_amount.collect()
for result in results:
    # print(f'Customer ID: {result[0]}, amount: {result[1]}')
    print(f'Total amount: {result[0]:.1f}, customer ID: {result[1]}')
