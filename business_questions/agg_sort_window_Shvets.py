import pyspark.sql.functions as f

from columns import primary_profession, id_person, genres, runtime_minutes


def top_10_professions_by_number_of_people(name_basics):
    """
    11. Top 10 most popular professions by number of people
    Args:
        name_basics: The name_basics dataframe.
    Returns:
        top_10_professions: dataframe with two columns: profession and people_number
    """
    profession = 'profession'
    people_number = 'people_number'
    top_10_professions = (name_basics
                          .withColumn(profession, f.explode(primary_profession))
                          .groupBy(profession)
                          .agg(f.countDistinct(id_person).alias(people_number))
                          .orderBy(people_number, ascending=False)
                          .limit(10))
    return top_10_professions


def average_runtime_per_genre(title_basics):
    """
    21. What is the average runtime for titles of each genre?
    Args:
        title_basics: The title_basics dataframe.
    Returns:
        average_runtime_per_genre_df: dataframe with two columns: genre and average_runtime
    """
    genre = 'genre'
    average_runtime = 'average_runtime'
    average_runtime_per_genre_df = (title_basics
                                    .withColumn(genre, f.explode(genres))
                                    .groupBy(genre)
                                    .agg(f.avg(runtime_minutes).alias(average_runtime))
                                    .orderBy(average_runtime, ascending=True))
    return average_runtime_per_genre_df
