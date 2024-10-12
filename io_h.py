from spark_session import spark_session
import pyspark.sql.types as t
from setting import write_path


def read_name_basics_df(path_to_df):
    """
    Read dataset name_basics
    :param path_to_df:
    :return: dataframe
    """
    spark = spark_session.getActiveSession()
    name_basics_schema = t.StructType([t.StructField('number', t.StringType(), False),
                                       t.StructField('name', t.StringType(), False),
                                       t.StructField('birth_year', t.IntegerType(), True),
                                       t.StructField('death_year', t.IntegerType(), True),
                                       t.StructField('primary_profession', t.StringType(), True),
                                       t.StructField('known_for_titles', t.StringType(), True), ])

    df = spark.read.csv(path_to_df, sep=r'\t', header=True, nullValue='null', schema=name_basics_schema)
    return df


def write_name_basics_to_csv(df_name_basics, write_path_to_df=write_path):
    """

    :param df_name_basics:
    :param write_path_to_df:
    :return:
    """

