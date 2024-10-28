import pyspark.sql.functions as f

from columns import primary_profession, genres, runtime_minutes


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


def get_people_with_only_2_professions(df):
    """
    19. People, who have only top-2 professions.
    Args:
        df: name_basics dataframe
    Returns:
        df: people with only top-2 professions
    """
    filtered_df = df.filter(f.array_size(f.col(primary_profession)) == 2)
    return filtered_df


def get_dramas_with_more_than_70_mins_runtime(df):
    """
    20. Which movies have drama genre and runtime minutes more than 70.
    Args:
        df: title_basics dataframe
    Returns:
        df: movies with drama genre and runtime minutes more than 70
    """
    filtered_df = df.filter((f.col(runtime_minutes) > 70)
                            & f.array_contains(f.col(genres), 'Drama'))
    return filtered_df
