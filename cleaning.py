from columns import *
import pyspark.sql.functions as f


def drop_col_birth_year_from_name_basics_df(df):
    """
    Drop column birth_year from dataframe name.basics
    Args:
        df (pyspark dataframe): dataframe name.basics
    Returns:
        (pyspark dataframe): modified dataframe name.basics with dropped column birth_year
    """
    return df.drop(birth_year)


def drop_col_death_year_from_name_basics_df(df):
    """
    Drop column death_year from dataframe name.basics

    Args:
         df (pyspark dataframe): dataframe name.basics

    Returns:
        (pyspark dataframe): modified dataframe name.basics with dropped column death_year
    """
    return df.drop(death_year)


def fill_col_end_year_in_title_basics_df(df):
    """
    Fill column end_year with values from column start_year in title.basics dataframe.

    Args:
        df (pyspark dataframe): dataframe title.basics

    Returns:
        (pyspark dataframe): modified dataframe title.basics with filled column end_year
    """
    return df.withColumn(end_year, f.when(f.col(end_year).isNull() & ~f.col(start_year).isNull(), f.col(start_year))
                         .otherwise(f.col(end_year)))
