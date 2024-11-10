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


def directors_highest_rated_action_movies_thousand_votes_since_twenty_fifteen(title_basics, title_ratings,
                                                                              title_crew, name_basics):
    """
    30. Which directors have worked on the highest-rated action movies with at least 1000 votes since 2015 or later?
    Args:
        title_basics: The title_basics dataframe.
        title_ratings: The title_ratings dataframe.
        title_crew: The title_crew dataframe.
        name_basics: The name_basics dataframe.
    Returns:
        action_directors: dataframe with 5 columns: primaryName, originalTitle, startYear, averageRating, numVotes
    """
    genre = 'genre'
    director = 'director'
    title_basics_filtered = title_basics.filter(f.col(start_year) >= 2015)
    title_ratings_filtered = title_ratings.filter(f.col(num_votes) >= 1000)
    title_crew_filtered = title_crew.filter(f.col(directors).isNotNull())
    action_directors = (title_basics_filtered
                        .withColumn(genre, f.explode(f.col(genres)))
                        .filter((f.col(genre) == 'Action') & (f.col(title_type) == 'movie'))
                        .join(title_ratings_filtered, tconst, how='left')
                        .join(title_crew_filtered, tconst)
                        .select(tconst, original_title, start_year, num_votes, average_rating, directors)
                        .withColumn(director, f.explode(f.col(directors))))
    action_directors = (action_directors
                        .join(name_basics, name_basics[id_person] == action_directors[director])
                        .select(primary_name, original_title, start_year, average_rating, num_votes)
                        .distinct()
                        .orderBy(f.col(average_rating).desc()))
    return action_directors
