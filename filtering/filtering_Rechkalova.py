# 17. Which titles have Ukrainian translations.
from columns import primary_profession, known_for_titles
import pyspark.sql.functions as f


# 15. People, who are actors and directors or actresses and directors at the same time.
def actors_or_actresses_and_directors_at_the_same_time(df):
    df = df.filter((f.array_contains(f.col(primary_profession), "actor") |
                    f.array_contains(f.col(primary_profession), "actress")) &
                   f.array_contains(f.col(primary_profession), "director"))
    return df


# 16. People who are known for only one title of movie.
def people_who_are_known_for_one_title_movie(df):
    df = df.filter(f.size(f.col(known_for_titles)) == 1)
    return df
