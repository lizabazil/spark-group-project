from columns import *
import pyspark.sql.functions as f


# title_akas
def drop_types_column(df):
    """
    To drop types column because it has 70% null values.

    Args:
        df (dataframe): The dataframe (title_akas).

    Returns:
        df: The modified title_akas dataframe.
    """
    df = df.drop(types)
    return df


def drop_attributes_column(df):
    """
    To drop attributes column because it has 99.4% null values.

    Args:
        df (dataframe): The dataframe (title_akas).

    Returns:
        df: The modified title_akas dataframe.
    """
    df = df.drop(attributes)
    return df


def fillna_region_language_with_unknown(df):
    """
    To fill null values in region and language columns with "unknown".

    Args:
        df (dataframe): The dataframe (title_akas).

    Returns:
        df: The modified title_akas dataframe.
    """
    df = df.fillna('unknown', subset=[region, language])
    return df


# title_episode
def drop_null_rows_episode(df):
    """
    To drop rows with null values in season_number and episode_number
    because they don't have any value for future analysis.

    Args:
        df (dataframe): The dataframe (title_episode).

    Returns:
        df: The modified title_episode dataframe.
    """
    df = df.filter(~(f.col(season_number).isNull() & f.col(episode_number).isNull()))
    return df


def drop_job_column_in_title_principals(df):
    """
    Drops job column in title_principals df because it has 81.25% null values
    Args:
        df: Dataframe title_principals.
    Returns:
        df: The title_principals dataframe without job column.
    """
    df_without_job_column = df.drop(job)
    return df_without_job_column


def drop_characters_column_in_title_principals(df):
    """
    Drops characters column in title_principals df because it has 51.5% null values, it is not used in questions
    Args:
        df: Dataframe title_principals.
    Returns:
        df: The title_principals dataframe without characters column.
    """
    df_without_characters_column = df.drop(characters)
    return df_without_characters_column


def drop_null_rows_in_title_crew(df):
    """
    Drops rows in title_crew df where both values (directors and writers) are null because they are useless
    Args:
        df: Dataframe title_crew.
    Returns:
        df: The title_crew dataframe without null rows.
    """
    df_without_null_rows = df.filter(f.col(directors).isNotNull() | f.col(writers).isNotNull())
    return df_without_null_rows


def drop_col_birth_year_from_name_basics_df(df):
    """
    Drop column birth_year from dataframe name.basics
    Args:
        df (pyspark dataframe): dataframe name.basics
    Returns:
        (pyspark dataframe): modified dataframe name.basics with dropped column birth_year
    """
    return df.drop(birth_year)


def drop_col_death_year_from_name_basics_df(df):
    """
    Drop column death_year from dataframe name.basics

    Args:
         df (pyspark dataframe): dataframe name.basics

    Returns:
        (pyspark dataframe): modified dataframe name.basics with dropped column death_year
    """
    return df.drop(death_year)


def fill_col_end_year_in_title_basics_df(df):
    """
    Fill column end_year with values from column start_year in title.basics dataframe.

    Args:
        df (pyspark dataframe): dataframe title.basics

    Returns:
        (pyspark dataframe): modified dataframe title.basics with filled column end_year
    """
    return df.withColumn(end_year, f.when(f.col(end_year).isNull() & ~f.col(start_year).isNull(), f.col(start_year))
                         .otherwise(f.col(end_year)))


def fill_col_runtime_minutes_in_title_basics(df):
    """
    Fill column runtime_minutes with mode of this column, depending on value in column  title_type.
    Args:
        df (pyspark dataframe): dataframe title.basics

    Returns:
        (pyspark dataframe): modified dataframe title.basics
    """
    grouped_sorted_by_titles_count_df = (df.groupBy(title_type, runtime_minutes).count()
                                         .orderBy(title_type, 'count', runtime_minutes, ascending=False))
    # the dataframe with two columns: 'title_type' and 'mode'. One title_type has only one row, and column 'mode'
    # represents mode based of column runtime_minutes
    modes_for_each_title_type_df = (grouped_sorted_by_titles_count_df.groupBy(title_type)
                                    .agg(f.first(runtime_minutes, ignorenulls=True).alias('mode')))

    modes_dict = dict(modes_for_each_title_type_df.rdd.map(lambda row: (row[title_type], row['mode'])).collect())

    filled_col_runtime_minutes_with_mode_df = df.withColumn(runtime_minutes, f.when(f.col(runtime_minutes).isNull(),
                                                                                    f.create_map([f.lit(x) for i in modes_dict.items() for x in i])[f.col(title_type)])
                                                            .otherwise(f.col(runtime_minutes))
                                                            )

    return filled_col_runtime_minutes_with_mode_df
