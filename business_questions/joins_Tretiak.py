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


def producers_worked_min_three_comedies_in_specified_period(title_basics_df, name_basics_df, title_principals_df):
    """
    Get the writers, who worked on at least 3 comedies between 1980 and 2010.
    Args:
        title_basics_df (pyspark DataFrame): DataFrame title.basics
        name_basics_df (pyspark DataFrame): DataFrame name.basics
        title_principals_df (pyspark DataFrame): DataFrame title.principals
    Returns:
        (pyspark DataFrame): DataFrame with writers, who worked on at least 3 comedies between 1980 and 2010. DataFrame
        contains columns nconst, primary_name and comedies_from_1980_to_2010 (its count).

    """
    # comedy titles made between 1980 and 2010
    only_comedy_titles_df = title_basics_df.filter((f.array_contains(f.col(genres), 'Comedy')) &
                                                   (f.col(start_year) >= 1980) &
                                                   (f.col(start_year) <= 2010))
    name_basics_only_writers_df = name_basics_df.filter(f.array_contains(f.col(primary_profession), 'writer'))

    # filter rows in title_principals_df, where category is writer (because we need only people, who worked on
    # titles as writers)
    title_principals_category_writer_df = title_principals_df.filter(f.col(category) == 'writer')

    # join by title id
    join_condition = title_principals_category_writer_df[tconst] == only_comedy_titles_df[tconst]
    principals_title_basics_joined_df = title_principals_category_writer_df.join(only_comedy_titles_df,
                                                                                 join_condition,
                                                                                 'inner')

    # group by each writer and count how many comedies he/she worked on
    alias_for_count = 'comedies_from_1980_to_2010'
    grouped_by_writer_df = principals_title_basics_joined_df.groupBy(nconst).agg(f.count('*').
                                                                                 alias(alias_for_count))
    min_three_comedies_for_writer_df = grouped_by_writer_df.filter(f.col(alias_for_count) >= 3)

    # join by person id
    join_condition = min_three_comedies_for_writer_df[nconst] == name_basics_only_writers_df[id_person]
    result_df = min_three_comedies_for_writer_df.join(name_basics_only_writers_df, join_condition, 'inner')

    ordered_by_nconst_df = (result_df.orderBy(f.col(nconst)).
                            select(nconst, primary_name,
                                   alias_for_count))
    return ordered_by_nconst_df
