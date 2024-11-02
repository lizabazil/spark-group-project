import pyspark.sql.functions as f
from columns import runtime_minutes, genres


def predominant_genres_of_movies_over_120_minutes(df):
    """
    6. Predominant genres of movies over 120 minutes.

    Args:
        df (dataframe): The title_basics dataframe.

    Returns:
         dataframe: New dataframe with two columns: genre and count (number of movies).
    """
    long_movies_by_genre = (df
                            .filter(f.col(runtime_minutes) > 120)
                            .withColumn("genre", f.explode(genres))
                            .groupBy("genre")
                            .count()
                            .orderBy(f.desc("count"))
                            )
    return long_movies_by_genre
