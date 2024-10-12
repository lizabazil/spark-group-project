from spark_session import spark_session
import pyspark.sql.types as t


def read_title_principals_df(path_to_df):
    """
    Read dataset title.basics
    :param path_to_df:
    :return dataframe
    """
    spark = spark_session.getActiveSession()
    title_principals_scheme = t.StructType(
        [t.StructField('tconst', t.StringType(), False),
         t.StructField('ordering', t.IntegerType(), False),
         t.StructField('nconst', t.StringType(), False),
         t.StructField('category', t.StringType(), False),
         t.StructField('job', t.StringType(), True),
         t.StructField('characters', t.StringType(), True)
         ])
    df = spark.read.csv(path_to_df, sep='\t', nullValue='null',
                        schema=title_principals_scheme, header=True)
    return df


