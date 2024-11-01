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
