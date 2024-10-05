from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from spark_session import spark_session


def basic_test_df():
    data = [('Help', 10), ('Aristotle And Dante Discover the Secretes of the Universe', 10), ('The Shining', 10),
            ('Educated', 10), ('Best Served Cold', 8)]
    schema = StructType([
        StructField('book_title', StringType(), True),
        StructField('rating', IntegerType(), True)
    ])
    df = spark_session.createDataFrame(data, schema)
    return df
