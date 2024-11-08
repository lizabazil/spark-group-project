from columns import *
import pyspark.sql.functions as f


def average_rating_for_horror_and_drama_titles_with_min_votes(title_ratings_df, title_basics_df):
    """
    Show average ratings only for horror and drama titles, which received at least 1000 votes.

    Args:
        title_ratings_df (pyspark DataFrame): DataFrame title.ratings
        title_basics_df (pyspark DataFrame): DataFrame title.basics
    Returns:
        (pyspark DataFrame): DataFrame with average rating for horror and drama titles, which received at least 1000
        votes.
    """
    filtered_ratings_with_min_votes_df = title_ratings_df.filter(f.col(num_votes) >= 1000)
    join_condition = (title_ratings_df[tconst] == title_basics_df[tconst]) & \
                     (f.array_contains(title_basics_df[genres], 'Horror') |
                      f.array_contains(title_basics_df[genres], 'Drama'))
    joined_df = filtered_ratings_with_min_votes_df.join(title_basics_df, join_condition, 'right')
    return joined_df
