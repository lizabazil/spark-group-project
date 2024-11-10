import pyspark.sql.functions as f
from columns import *


def get_titles_made_in_specific_decade(title_basics_df):
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


def get_titles_of_short_comedies(title_basics_df):
    """
    Get the titles of short comedies

    Args:
        title_basics_df (pyspark DataFrame): DataFrame title.basics

    Returns:
        (pyspark DataFrame): filtered DataFrame with titles of short comedies

    """
    titles_of_short_comedy_films_df = title_basics_df.filter((f.col(title_type) == 'short') &
                                                             (f.array_contains(f.col(genres), 'Comedy')))
    return titles_of_short_comedy_films_df


def get_titles_with_three_genres(title_basics_df):
    """
    Get the titles, which have 3 genres

    Args:
         title_basics_df (pyspark DataFrame): DataFrame title.basics

    Returns:
        (pyspark DataFrame): filtered DataFrame with titles, which have 3 genres
    """
    titles_with_3_genres_df = title_basics_df.filter(f.size(f.col(genres)) == 3)
    return titles_with_3_genres_df
