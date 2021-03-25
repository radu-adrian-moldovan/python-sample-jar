import random
from pyspark import SparkContext, SQLContext
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

TEST = "xxx_test"

def my_print(x):
    print(x)


def sample_function(sc: SparkContext):
    schema = StructType([StructField("odd_numbers", IntegerType(), True)])

    print(" Odds number sample")
    big_list = range(10)
    rdd = sc.parallelize(big_list, 2)
    odds = rdd.filter(lambda x: x % 2 != 0)
    odds.foreach(my_print)
    sql_context = SQLContext(sc)
    odd_numbers = sql_context.createDataFrame(odds.map(lambda _: Row(_)), schema)
    odd_numbers.printSchema()
    odd_numbers.show(truncate=False)
    print("odd_numbers count:" + str(odd_numbers.count()))

    odd_numbers.createOrReplaceTempView("odd_numbers_table")
    sql_context.sql("select * from odd_numbers_table limit 2;").show()

    return (odd_numbers)

if __name__ == '__main__':
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> sample_function <<<<<<<<<<<<<<<<<<<<<<<<<")
