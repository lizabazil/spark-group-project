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
    name_basics_schema = t.StructType([t.StructField('nconst', t.StringType(), False),
                                       t.StructField('primaryName', t.StringType(), False),
                                       t.StructField('birthYear', t.IntegerType(), True),
                                       t.StructField('deathYear', t.IntegerType(), True),
                                       t.StructField('primaryProfession', t.StringType(), True),
                                       t.StructField('knownForTitles', t.StringType(), True), ])

    df = spark.read.csv(path_to_df, sep=r'\t', header=True, nullValue='null', schema=name_basics_schema)
    return df


def write_name_basics_to_csv(df_name_basics, write_path_to_df=write_path):
    """
    Writes the dataframe to csv file.
    :param df_name_basics:
    :param write_path_to_df:
    :return: None
    """
    df_name_basics.write.csv(write_path_to_df, mode='overwrite', header=True)
    return None


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


def read_title_episode_df(path_to_df):
    """
    Read title.episode.tsv file and return
    :param path_to_df:
    :return: dataframe
    """
    title_episode_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                      t.StructField('parentTconst', t.StringType(), False),
                                      t.StructField('seasonNumber', t.IntegerType(), True),
                                      t.StructField('episodeNumber', t.IntegerType(), True),])

    spark = spark_session.getActiveSession()
    from_tsv_default_df = spark.read.csv(path_to_df, sep=r"\t", header=True, nullValue='null',
                                         schema=title_episode_schema)
    return from_tsv_default_df


def write_title_episode_df_to_csv(df, path=write_path):
    """
    Write dataframe title.episode.tsv
    :param df:
    :param path:
    :return:
    """
    df.write.csv(path, header=True, mode="overwrite")


def read_title_crew_df(path_to_df):
    """
    Read dataset title_crew
    :param path_to_df:
    :return: dataframe
    """
    spark = spark_session.getActiveSession()
    title_crew_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                      t.StructField('directors', t.StringType(), True),
                                      t.StructField('writers', t.StringType(), True)])
    df = spark.read.csv(path_to_df, sep=r'\t', header=True, nullValue='null', schema=title_crew_schema)
    return df


def write_title_crew_df_to_csv(df, path=write_path):
    """
    Write title_crew dataframe to csv file
    :param df:
    :param path:
    :return:
    """
    df.write.csv(path, header=True, mode="overwrite")


def read_title_ratings_df(path_to_df):
    """
    Read dataset title_ratings
    :param path_to_df:
    :return: dataframe
    """
    spark = spark_session.getActiveSession()
    title_ratings_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                         t.StructField('averageRating', t.DoubleType(), False),
                                         t.StructField('numVotes', t.IntegerType(), False)])

    df = spark.read.csv(path_to_df, sep=r'\t', header=True, nullValue='null', schema=title_ratings_schema)
    return df


def write_title_ratings_df_to_csv(df, path=write_path):
    """
    Write title_ratings dataframe to csv file
    :param df:
    :param path:
    :return:
    """
    df.write.csv(path, header=True, mode="overwrite")
