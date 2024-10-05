from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t

spark_session = (SparkSession.builder
                 .master("local")
                 .appName("test app")
                 .config(conf=SparkConf())
                 .getOrCreate())

