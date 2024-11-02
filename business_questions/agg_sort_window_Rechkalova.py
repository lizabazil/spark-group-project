import pyspark.sql.functions as f
from columns import runtime_minutes, genres, start_year, title_type


def predominant_genres_of_movies_over_120_minutes(df):
    """
    6. What are the predominant genres of movies over 120 minutes?

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


def average_release_year_by_type(df):
    """
    7. What is the average year of release for each type of film?

    Args:
        df (dataframe): The title_basics dataframe.

    Returns:
        dataframe: New dataframe with two columns: title_type and average_release_year.
    """
    avg_release_year_by_type = (df
                                .filter(f.col(start_year).isNotNull())
                                .groupBy(title_type)
                                .agg(f.round(f.avg(start_year).alias("average_release_year")))
                                )
    return avg_release_year_by_type
