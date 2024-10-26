from columns import types, attributes, season_number, episode_number
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

