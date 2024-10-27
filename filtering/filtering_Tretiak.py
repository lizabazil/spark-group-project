import pyspark.sql.functions as f
from columns import *


def get_titles_made_between_1950_and_1960(title_basics_df):
    """
    Return the movies made between 1950 and 1960 (including 1950 and 1960 years)

    Args:
        title_basics_df (pyspark DataFrame): DataFrame title.basics

    Returns:
        (pyspark DataFrame): filtered DataFrame with movies made between 1950 and 1960.
    """
    titles_from_1950_to_1960_df = title_basics_df.filter((f.col(start_year) >= 1950) & (f.col(start_year) <= 1960)
                                                         & (f.col(end_year) <= 1960))
    return titles_from_1950_to_1960_df
