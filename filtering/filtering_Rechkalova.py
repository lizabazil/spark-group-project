# 16. People who are known for only one title of movie.
# 17. Which titles have Ukrainian translations.
from columns import primary_profession
import pyspark.sql.functions as f


# 15. People, who are actors and directors or actresses and directors at the same time.
def actors_or_actresses_and_directors_at_the_same_time(df):
    df = df.filter((f.array_contains(f.col(primary_profession), "actor") |
                    f.array_contains(f.col(primary_profession), "actress")) &
                   f.array_contains(f.col(primary_profession), "director"))
    return df
