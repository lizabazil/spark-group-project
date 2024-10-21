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


def drop_end_year_col(title_basics_df):
    """
    To delete column 'end_year' from the dataframe, as there are too many null values
    in this column (more than 70% of rows in dataframe).

    Args:
        title_basics_df: dataframe

    Returns:
        (pyspark dataframe): modified dataframe
    """
    return title_basics_df.drop(end_year)


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
