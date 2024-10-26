import re
import pyspark.sql.functions as f


def change_column_names_to_snake_case(df):
    """
    To change the column names to snake_case style.

    Args:
        df (dataframe): The dataframe.

    Returns:
        dataframe with the new changed column names.

    """
    new_columns_names = [to_snake_case(col) for col in df.columns]
    for idx, old_col in enumerate(df.columns):
        df = df.withColumnRenamed(old_col, new_columns_names[idx])
    return df


def to_snake_case(name_column):
    """
    To modify column name style to snake_case style.

    Args:
        name_column (string): The column name.

    Returns:
        string: The modified column name.
    """
    new_column_name = re.sub(r'(?<!^)(?=[A-Z])', '_', name_column)
    return new_column_name.lower()


def null_from_string_to_none(df):
    r"""
    To change all \N (strings) to None values.

    Args:
        df (dataframe): The dataframe.

    Returns:
        Dataframe with correctly defined null values.

    """
    for col in df.columns:
        df = df.withColumn(col, f.when(f.col(col).isin(r'\N'), None).otherwise(f.col(col)))
    return df
