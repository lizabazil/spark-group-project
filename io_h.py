from spark_session import spark_session
import pyspark.sql.types as t


def read_title_akas_df(path_to_df):
    """
    Read title.akas.tsv file and return
    :param path_to_df:
    :return: dataframe
    """
    title_akas_schema = t.StructType([t.StructField('titleId', t.StringType(), False),
                                      t.StructField('ordering', t.IntegerType(), False),
                                      t.StructField('title', t.StringType(), False),
                                      t.StructField('region', t.StringType(), True),
                                      t.StructField('language', t.StringType(), True),
                                      t.StructField('types', t.StringType(), True),
                                      t.StructField('attributes', t.StringType(), True),
                                      t.StructField('isOriginalTitle', t.IntegerType(), False),])

    spark = spark_session.getActiveSession()
    from_tsv_default_df = spark.read.csv(path_to_df, sep=r"\t", header=True, nullValue='null', schema=title_akas_schema)
    return from_tsv_default_df
