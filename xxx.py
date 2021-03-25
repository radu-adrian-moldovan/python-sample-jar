import random
from pyspark import SparkContext, SQLContext
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from time import time
import numpy as np
from random import random
from operator import add
from pyspark.sql import SparkSession

TEST = "xxx_test"

def my_print(x):
    print(x)
    
print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> before <<<<<<<<<<<<<<<<<<<<<<<<<")


spark = SparkSession.builder.appName('xxx CalculatePi').getOrCreate()
sc = spark.sparkContext

def is_point_inside_unit_circle(p):
    # p is useless here
    x, y = random(), random()
    return 1 if x*x + y*y < 1 else 0


t_0 = time()

# parallelize creates a spark Resilient Distributed Dataset (RDD)
# its values are useless in this case
# but allows us to distribute our calculation (inside function)
count = sc.parallelize(range(0, n)) \
             .map(is_point_inside_unit_circle).reduce(add)
print(np.round(time()-t_0, 3), "seconds elapsed for spark approach and n=", n)
print("Pi is roughly %f" % (4.0 * count / n))

# VERY important to stop SparkSession
# Otherwise, the job will keep running indefinitely
spark.stop()





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

print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> after <<<<<<<<<<<<<<<<<<<<<<<<<")
if __name__ == "__main__":
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> sample_function <<<<<<<<<<<<<<<<<<<<<<<<<")

