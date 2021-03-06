# Spark

## Idea

- Nothing is going to occur until actions are called, only creating Directed Acyclic Grapgh (DAG), one of the reasons Spark is fast. Actions are for example reduceByKey(), collect()

## Debugging

- Spark won't do anything until actions are called. To debug the intermediate steps, you need to call show() to print
  the data. But this spends unnecessary resources in the middle. So after debugging, comment out all the debuging print
  statements.

## Submit a job

In terminal, `$ spark-submit PYTHON_FILE_NAME.py`

## Resilient Distributed Dataset (RDD) 
 
- RDD could be a good choice if the data is a type of unstructured data requiring mapReduce() type of operations.
- RDD is still a part of Spark 3.

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

It's a good practice to discard the unnecessary information as early as possible to optimize cluster resource.

For example, use select('COLUMN NAME') to limit the columns only you need.

- DataFrame is a good choice if the data is structured data.
- withColumn(NAME, DATA) creates a new column.
- Filter `DF.filter(func.col('COLUMN_NAME') == 'VALUE')`
- Join `DF.join(OTHER_DF, 'COMMON_COLUMN_NAME')`

## SQL Functions

- Import it by `from pyspark.sql import functions as func`
- func.explode() explodes columns into rows, similar to flatmap. Each data horizontally in columns will be rows 
  vertically
- [Aggregate, apply function, and rename the column](https://github.com/yukikitayama/spark/blob/main/exercise/total_amount_by_customer_dataframe.py)

## Schema

- Use it to provide column names if the data doesn't have a header.

## Mapping

- Keep a dictionary loaded in the driver program
- Use `sc.broadcast()` to broadcast objects to the executors
- Use `BROADCASTED_OBJECT.value()` to get the object back.

## UDF

- [UDF example](https://github.com/yukikitayama/spark/blob/main/activity/popular-movies-nice-dataframe.py)

## Accumulator

- Shared variable by executors in a cluster

## Caching

- When you do multiple actions on a dataframe, you should cache it to avoid re-evaluating the entire dataframe all over
  again
- .cache()
  - keep it in memory
- .persist()
  - Optionally let you cache it to disk instead of memory, useful when a node fails.

## Configuration

- `SparkSession.builder.master('local[*]')`
  - `local[*]` means use every CPU core on a local system to execute a job. It makes sense to use if in your laptop, but
    in a production cluster, it doesn't utilize other machines in a cluster.

## Partitioning

- Using `.partitionBy()` on an RDD before running a large operation benefits from partitioning.
- Should consider use it if you use any of the following methods
  - `join(), cogroup(), groupWith(), join(), leftOuterJoin(), rightOuterJoin(), groupByKey(), reduceByKey(),
    combineByKey(), lookup()`
- `partitionBy(100)` is a reasonable start if you have 5 to 10 computers

## Machine Learning Library

- xxx

## Spark Streaming

- Dstream
  - Objects broken up the stream into distinct RDD's
  - Micro batch
