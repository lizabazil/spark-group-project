from columns import *
import pyspark.sql.functions as f


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
