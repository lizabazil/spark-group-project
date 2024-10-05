from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from spark_session import spark_session


def basic_test_df():
    data = [('cat', 1), ('kitty', 2), ('meow', 3)]
    schema = StructType([
        StructField('Name', StringType(), True),
        StructField('Age', IntegerType(), True)
    ])
    df = spark_session.createDataFrame(data, schema)
    return df

