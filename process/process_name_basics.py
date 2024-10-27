import pyspark.sql.functions as f
from columns import *


def make_primary_profession_col_array_type(name_basics_df):
    """
    To modify the column  'primary_profession' type to array type.

    Args:
        name_basics_df (pyspark dataframe): The name_basics dataframe.

    Returns:
        pyspark dataframe: dataframe with the modified column type 'primary_profession'.
    """
    name_basics_df = name_basics_df.withColumn(primary_profession,
                                               f.split(name_basics_df[primary_profession], ','))
    return name_basics_df


def make_known_for_titles_col_array_type(name_basics_df):
    """
    To modify the column  'known_for_titles' type to array type.

    Args:
        name_basics_df (pyspark dataframe): The name_basics dataframe.

    Returns:
        pyspark dataframe: dataframe with the modified column type 'known_for_titles'.
    """
    name_basics_df = name_basics_df.withColumn(known_for_titles,
                                               f.split(name_basics_df[known_for_titles], ','))
    return name_basics_df


def rename_nconst_col(name_basics_df):
    """
    Rename column 'nconst' to 'id_person'.

    Args:
        name_basics_df (pyspark dataframe): The name_basics dataframe.

    Returns:
        pyspark dataframe: dataframe with the modified column name 'id_person'.
    """
    name_basics_df = name_basics_df.withColumnRenamed(nconst, id_person)
    return name_basics_df
