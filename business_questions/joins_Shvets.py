import pyspark.sql.functions as f

from columns import (tconst, id_person, genres, average_rating, start_year, title_type, num_votes, directors,
                     original_title, primary_name)


def get_genres_with_highest_rating(title_basics, title_ratings):
    """
    29. Which genres have the highest average title rating?
    Args:
        title_basics: The title_basics dataframe.
        title_ratings: The title_ratings dataframe.
    Returns:
        title_genre_ratings: dataframe with two columns: genre and avg_rating
    """
    genre = 'genre'
    avg_rating = 'avg_rating'
    title_genre_ratings = (title_basics.join(title_ratings, tconst)
                           .withColumn(genre, f.explode(f.col(genres)))
                           .groupBy(genre)
                           .agg(f.avg(average_rating).alias(avg_rating))
                           .orderBy(f.col(avg_rating).desc())
                           .select(genre, avg_rating))
    return title_genre_ratings
