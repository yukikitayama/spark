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
