import pyspark.sql.functions as f
from pyspark.sql import Window

from columns import primary_profession, id_person, genres, runtime_minutes, start_year, original_title, language, title


def top_ten_professions_by_number_of_people(name_basics):
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


def animated_fantasy_films_count_per_decade(title_basics):
    """
    22. How many animated fantasy films were released in total each decade?
    Args:
        title_basics: The title_basics dataframe.
    Returns:
        animated_fantasy_decades: dataframe with two columns: decade and movies_number
    """
    decade = 'decade'
    animated_fantasy_decades = (title_basics
                                .filter(f.array_contains(f.col(genres), 'Animation')
                                        & f.array_contains(f.col(genres), 'Fantasy'))
                                .withColumn(decade,
                                            (f.col(start_year) - (f.col(start_year) % 10)))
                                .groupBy(decade)
                                .count()
                                .withColumnRenamed('count', 'movies_number')
                                .orderBy(decade, ascending=True))
    return animated_fantasy_decades


def top_three_long_runtime_titles_per_decade(title_basics):
    """
    23. Top 3 titles with the longest runtime for each decade
    Args:
        title_basics: The title_basics dataframe.
    Returns:
        top_long_runtime_titles: dataframe with 4 columns: decade, original_title, runtime_minutes, rank
    """
    decade = 'decade'
    rank = 'rank'
    decade_runtime_window = Window.partitionBy(decade).orderBy(f.desc(runtime_minutes))
    top_long_runtime_titles = (title_basics
                               .withColumn(decade,
                                           (f.col(start_year) - (f.col(start_year) % 10)))
                               .withColumn(rank, f.row_number().over(decade_runtime_window))
                               .select(decade, original_title, runtime_minutes, rank)
                               .filter((f.col(rank) <= 3)))
    return top_long_runtime_titles


def analyze_title_length_for_each_lang(title_akas):
    """
    24. Find longest and shortest title length, mode of lengths and its percent rank for each language
    Args:
        title_akas: The title_akas dataframe.
    Returns:
        result: dataframe with 5 columns: language, max_length, min_length, mode_length, mode_percent_rank
    """
    length = 'length'
    mode_length = 'mode_length'
    mode_length_window = Window.orderBy(mode_length)
    result = (title_akas
              .filter(f.col(language) != "unknown")
              .withColumn(length, f.length(title))
              .groupBy(language)
              .agg(f.max(length).alias("max_length"),
                   f.min(length).alias("min_length"),
                   f.expr("mode(length)").alias(mode_length))
              .withColumn("mode_percent_rank",
                          f.round(f.percent_rank().over(mode_length_window), 2))
              .orderBy(language))
    return result
