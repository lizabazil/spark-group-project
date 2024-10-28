import pyspark.sql.functions as f

from columns import primary_profession


def get_directors_not_producers(df):
    """
    18. People, who are directors, but not producers.
    Args:
        df: name_basics dataframe
    Returns:
        df: dataframe with only people who are directors but not producers
    """
    filtered_df = df.filter(f.array_contains(f.col(primary_profession), 'director')
                            & ~f.array_contains(f.col(primary_profession), 'producer'))
    return filtered_df
