import pyspark.sql.functions as f
from columns import *


def make_genres_array_type(df):
    """
    Make column 'genres' array type.

    Args:
         df: dataframe

    Returns:
        (pyspark dataframe)
    """
    df = df.withColumn(genres, f.split(df[genres], ','))
    return df


def convert_is_adult_col_to_boolean_type(title_basics_df):
    """
    Convert column 'is_adult' type to boolean type.

    Args:
        title_basics_df: dataframe

    Returns:
        (pyspark dataframe): modified dataframe
    """
    title_basics_df = title_basics_df.withColumn(is_adult, f.when(f.col(is_adult) == 1, True).otherwise(False))
    return title_basics_df
