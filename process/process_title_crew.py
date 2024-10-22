from columns import directors, writers
import pyspark.sql.functions as f


def convert_directors_col_to_array(title_crew_df):
    """
    Converts directors column from string to array
    :param title_crew_df:
    :return: title_crew_df
    """
    title_crew_df = title_crew_df.withColumn(directors,
                                             f.split(title_crew_df[directors], ','))
    return title_crew_df


def convert_writers_col_to_array(title_crew_df):
    """
    Converts directors column from string to array
    :param title_crew_df:
    :return: title_crew_df
    """
    title_crew_df = title_crew_df.withColumn(writers,
                                             f.split(title_crew_df[writers], ','))
    return title_crew_df
