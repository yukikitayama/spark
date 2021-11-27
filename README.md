# Spark

## Idea

- Nothing is going to occur until actions are called, only creating Directed Acyclic Grapgh (DAG), one of the reasons Spark is fast. Actions are for example reduceByKey(), collect()

## Submit a job

In terminal, `$ spark-submit PYTHON_FILE_NAME.py`

## Resilient Distributed Dataset (RDD) 

RDD can contains
- Collection of single values. For example, lines of texts
- Collection of key value pairs. For example, (1, x), (2, y), ...
  - To use key value methods such as mapValues(lambda function), reduceByKey(lambda function)

## map() vs. flatMap()

- map() transforms each element of an RDD into one new element.
- flatMap() can create many new elements from each one.

## DataFrame

The trend in Spark is to use RDDs less and DataFrames more because it's compatible with MLLib and Spark Streaming, and
also it allows users to run query to data.
DataFrame is a collection of Row objects, a structured data object extending RDD, can run SQL and have a schema which leads to more efficient storage.
Use SparkSession instead of SparkContext
In Spark 2+, DataFrame is a DataSet of Row objects.
