from spark_session import spark_session
import pyspark.sql.types as t
from setting import write_path


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


def write_title_akas_df_to_csv(df, path=write_path):
    """
    Write dataframe title.akas.tsv
    :param df:
    :param path:
    :return:
    """
    df.write.csv(path, header=True, mode="overwrite")

    
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


def write_title_principals_df_to_csv(df, path=write_path):
    """
    Write title_principals dataframe to csv file
    :param df:
    :param path:
    :return: None
    """
    df.write.csv(path, header=True, mode="overwrite")
