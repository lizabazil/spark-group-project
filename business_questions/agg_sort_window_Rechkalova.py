import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from columns import (runtime_minutes, genres, start_year, title_type, original_title, tconst, region, ordering,
                     title)


def predominant_genres_of_movies_over_one_twenty_minutes(df):
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
                                .agg(f.round(f.avg(start_year)).cast(t.IntegerType()).alias("average_release_year"))
                                )
    return avg_release_year_by_type


def tvmovies_per_year_after_nineties(df):
    """
    8. What is the number of movies made every year after 1990, only of the "tvMovie" type?

    Args:
        df (dataframe): The title_basics dataframe.

    Returns:
        dataframe: New dataframe with two columns: start_year and count.
    """
    tvmovies_per_year = (df
                         .filter((f.col(start_year) > 1990) & (f.col(title_type) == "tvMovie"))
                         .groupBy(start_year)
                         .count()
                         .orderBy(start_year)
                         )
    return tvmovies_per_year


def average_runtime_for_every_type(df):
    """
    9. What is the average runtime for every tvType?

    Args:
        df (dataframe): The title_basics dataframe.

    Returns:
        dataframe: New dataframe with two columns: title_type and average_runtime.
    """
    window = Window.partitionBy(title_type)
    average_runtime_df = (df
                          .withColumn("average_runtime", f.avg(runtime_minutes).over(window))
                          .select(title_type, "average_runtime").distinct())
    return average_runtime_df


def top_five_the_longest_drama_films_after_two_thousand(df):
    """
    What are the longest drama films from the year 2000 by title type,
    which fall within the top 5% of the longest films of their type?

    Args:
        df (dataframe): The title_basics dataframe.

    Returns:
        dataframe: New dataframe with columns: tconst, original_title, title_type, runtime_minutes, and cume_dist,
                   representing the top 5% of longest drama films for each title type from the year 2000.
    """
    filtered_df = df.filter(
        (f.col(start_year) == '2000') &
        (f.array_contains(f.col(genres), 'Drama'))
    )
    window_spec = Window.partitionBy(title_type).orderBy(f.col(runtime_minutes).desc())
    ranked_df = filtered_df.withColumn('cume_dist', f.cume_dist().over(window_spec))
    top_titles_df = ranked_df.filter(f.col('cume_dist') <= 0.05) .select(tconst, original_title, title_type,
                                                                         runtime_minutes, 'cume_dist')
    return top_titles_df