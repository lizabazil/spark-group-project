from columns import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window


def longest_runtime_time_per_title_type(title_basics_df):
    """
    Get the longest runtime time per title type (ordered by runtime time in descending order).

    Args:
        title_basics_df (pyspark DataFrame): DataFrame title.basics
    Returns:
        (pyspark DataFrame): DataFrame with the longest runtime time per title type (ordered by runtime time in
        descending order).
    """
    longest_runtime_per_title_type_df = (title_basics_df.groupBy(title_type).agg(f.max(runtime_minutes)
                                                                                 .alias('max_runtime_minutes')))
    ordered_longest_runtime_per_title_type_df = longest_runtime_per_title_type_df.orderBy('max_runtime_minutes',
                                                                                          ascending=False)
    return ordered_longest_runtime_per_title_type_df


def amount_of_non_adult_titles_each_type_every_year(title_basics_df):
    """
    Get the dataframe with the amount of non-adult titles of each title type made every year.

    Args:
        title_basics_df (pyspark DataFrame): DataFrame title.basics
    Returns:
        (pyspark DataFrame): DataFrame with the amount of non-adult titles of each title type made every year.
    """
    # filter non-adult titles
    non_adult_titles_df = title_basics_df.filter(f.col(is_adult) == False)

    # filter rows, where column start_year is not null
    col_start_year_not_null_df = non_adult_titles_df.filter(~f.col(start_year).isNull())
    amount_titles_by_year_and_title_type_df = col_start_year_not_null_df.groupBy(title_type, start_year).count()
    ordered_df = amount_titles_by_year_and_title_type_df.orderBy([title_type, start_year],
                                                                 ascending=[True, False])
    return ordered_df


def amount_adult_and_non_adult_titles_per_title_type(title_basics_df):
    """
    Get the amount of adult and non-adult titles per title type.

    Args:
        title_basics_df (pyspark DataFrame): DataFrame title.basics
    Returns:
        (pyspark DataFrame): DataFrame with the amount of adult and non-adult titles per title type.
    """
    amount_adult_and_not_adult_per_title_type_df = title_basics_df.groupBy(title_type, is_adult).count()
    ordered_df = amount_adult_and_not_adult_per_title_type_df.orderBy([title_type, is_adult], ascending=[True, False])
    return ordered_df


def change_of_titles_amount_from_prev_year(title_basics_df):
    """
    Get the change of the amount of titles from the previous year for each title type.

    Args:
        title_basics_df (pyspark DataFrame): DataFrame title.basics
    Returns:
        (pyspark DataFrame): DataFrame with the change of the amount of titles from the previous year.
    """
    # filter rows, where column start_year is not null
    col_start_year_not_null_df = title_basics_df.filter(~f.col(start_year).isNull())
    with_amount_of_titles_per_year_df = col_start_year_not_null_df.groupBy(start_year, title_type).count()

    window = Window.partitionBy(title_type).orderBy(start_year)
    # add column with the amount of titles from the previous year
    df = with_amount_of_titles_per_year_df.withColumn('prev_year_count', f.lag('count').over(window))
    df = df.withColumn('title_amount_change', f.col('count') - f.col('prev_year_count'))
    return df


def top_10_percent_titles_with_longest_runtime_per_type(title_basics_df):
    """
    Get the top 10% titles with the longest runtime time per title type.

    Args:
        title_basics_df (pyspark DataFrame): DataFrame title.basics
    Returns:
        (pyspark DataFrame): DataFrame with the top 10% titles with the longest runtime time per title type.
    """
    window_spec = Window.partitionBy(title_type).orderBy(runtime_minutes)
    df = title_basics_df.withColumn('cume_dist', f.cume_dist().over(window_spec))
    df = df.filter(f.col('cume_dist') >= 0.9)
    return df

