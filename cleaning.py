from columns import *
import pyspark.sql.functions as f


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
