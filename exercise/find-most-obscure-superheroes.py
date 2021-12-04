"""
- List the names of all superheroes with only one connection
- Compute the actual smallest number of connections in the data set instead of assuming it it one.

- df.filter(func.col('COLUMN_NAME') == 'SOME_VALUE')
- df.join(OTHER_DF, 'COMMON_COLUMN_NAME')
- .agg(func.min('COLUMN_NAME')).first()[0]
"""


from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder.appName('MostObscureSuperhero').getOrCreate()

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True)
])

names = spark.read.schema(schema).option('sep', ' ').csv('file:///SparkCourse/data/Marvel-names.txt')
lines = spark.read.text('file:///SparkCourse/data/Marvel-graph.txt')

connections = lines \
    .withColumn('id', func.split(func.trim(func.col('value')), ' ')[0]) \
    .withColumn('connections', func.size(func.split(func.trim(func.col('value')), ' ')) - 1) \
    .groupBy('id') \
    .agg(func.sum('connections').alias('connections'))

# The returned object is not dataframe, just a value
min_connection_count = connections \
    .agg(func.min('connections')) \
    .first()[0]

print(f'min_connection_count: {min_connection_count}')

min_connections = connections.filter(func.col('connections') == min_connection_count)

min_connections_with_names = min_connections.join(names, 'id')

print(f'The following characters have only: {min_connection_count} connection')

min_connections_with_names.show()

spark.stop()
