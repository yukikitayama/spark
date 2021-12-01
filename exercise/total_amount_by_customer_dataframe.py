from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql import functions as func

spark = SparkSession.builder.appName('TotalAmountByCustomerDataframe').master('local[*]').getOrCreate()

schema = StructType([
    StructField('customer_id', IntegerType(), True),
    StructField('item_id', IntegerType(), True),
    StructField('amount', FloatType(), True)
])

df = spark.read.schema(schema).csv('file:///SparkCourse/data/customer-orders.csv')
df.printSchema()

customer_amount = df.select('customer_id', 'amount')

# Group by customer ID, sum by amount, sort by total amount
total_amount_customer = customer_amount\
    .groupBy('customer_id')\
    .agg(func.round(func.sum('amount'), 2).alias('total_amount'))\
    .sort('total_amount')

total_amount_customer.show(total_amount_customer.count())

spark.stop()
