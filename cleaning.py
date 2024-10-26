from columns import *


def drop_col_birth_year_from_name_basics_df(df):
    """
    Drop column birth_year from dataframe name.basics
    Args:
        df (pyspark dataframe): dataframe name.basics
    Returns:
        (pyspark dataframe): modified dataframe name.basics with dropped column birth_year
    """
    return df.drop(birth_year)
