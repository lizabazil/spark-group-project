# title_akas
from columns import types, attributes


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

