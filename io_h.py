from spark_session import spark_session
import pyspark.sql.types as t
from columns import *
import pyspark.sql.functions as f


def write_dataframe_to_csv(df, write_path_to_df):
    """
    Writes the dataframe to csv file.
    :param df:
    :param write_path_to_df:
    :return: None
    """
    df.write.csv(write_path_to_df, mode='overwrite', header=True)
    return None


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


def write_name_basics_to_csv(name_basics_df, write_path):
    """
    Write dataframe name.basics to csv file.

    Args:
         name_basics_df: name.basics dataframe
         write_path: path to write

    Returns:
        None
    """
    name_basics_df = name_basics_df.withColumn(primary_profession,
                                               f.concat_ws(',', f.col(primary_profession)))
    name_basics_df = name_basics_df.withColumn(known_for_titles,
                                               f.concat_ws(',', f.col(known_for_titles)))
    name_basics_df.write.csv(write_path, mode='overwrite', header=True)
    return None


def write_title_basics_to_csv(title_basics_df, write_path):
    """
    Write dataframe title.basics to csv file.

    Args:
        title_basics_df: name.basics dataframe
        write_path: path to write

    Returns:
        None
        """
    title_basics_df = title_basics_df.withColumn(genres,
                                                 f.concat_ws(',', f.col(genres)))
    title_basics_df.write.csv(write_path, mode='overwrite', header=True)
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
                                      t.StructField('isOriginalTitle', t.IntegerType(), False), ])

    spark = spark_session.getActiveSession()
    from_tsv_default_df = spark.read.csv(path_to_df, sep=r"\t", header=True, nullValue='null', schema=title_akas_schema)
    return from_tsv_default_df


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


def read_title_basics_df(path_to_df):
    """
    To read the dataset title.basics.tsv

    Args:
         path_to_df: the path to the dataset.

    Returns:
        dataframe
    """
    spark = spark_session.getActiveSession()
    title_basics_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                        t.StructField('titleType', t.StringType(), False),
                                        t.StructField('primaryTitle', t.StringType(), False),
                                        t.StructField('originalTitle', t.StringType(), False),
                                        t.StructField('isAdult', t.IntegerType(), False),
                                        t.StructField('startYear', t.IntegerType(), True),
                                        t.StructField('endYear', t.IntegerType(), True),
                                        t.StructField('runtimeMinutes', t.IntegerType(), True),
                                        t.StructField('genres', t.StringType(), True)
                                        ])

    df = spark.read.csv(path_to_df, sep=r'\t', header=True, nullValue='null', schema=title_basics_schema)
    return df


def read_title_episode_df(path_to_df):
    """
    Read title.episode.tsv file and return
    :param path_to_df:
    :return: dataframe
    """
    title_episode_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                         t.StructField('parentTconst', t.StringType(), False),
                                         t.StructField('seasonNumber', t.IntegerType(), True),
                                         t.StructField('episodeNumber', t.IntegerType(), True), ])

    spark = spark_session.getActiveSession()
    from_tsv_default_df = spark.read.csv(path_to_df, sep=r"\t", header=True, nullValue='null',
                                         schema=title_episode_schema)
    return from_tsv_default_df


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
