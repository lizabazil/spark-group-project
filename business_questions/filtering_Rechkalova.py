from columns import primary_profession, known_for_titles, region, language
import pyspark.sql.functions as f


def actors_or_actresses_and_directors_at_the_same_time(df):
    """
    15. People, who are actors and directors or actresses and directors at the same time.

    Args:
        df (dataframe): The dataframe.

    Returns:
        dataframe: Filtered dataframe.
    """
    df = df.filter((f.array_contains(f.col(primary_profession), "actor") |
                    f.array_contains(f.col(primary_profession), "actress")) &
                   f.array_contains(f.col(primary_profession), "director"))
    return df


def people_who_are_known_for_one_title_movie(df):
    """
    16. People who are known for only one title of movie.

    Args:
        df (dataframe): The dataframe.

    Returns:
         dataframe: Filtered dataframe.
    """
    df = df.filter(f.size(f.col(known_for_titles)) == 1)
    return df


def titles_with_ukrainian_translation(df):
    """
    17. Which titles have Ukrainian translations (regions and language columns).

    Args:
        df (dataframe): The dataframe.

    Returns:
         dataframe: Filtered dataframe.
    """
    df = df.filter((f.col(region) == "UA") | (f.col(language) == "uk"))
    return df
