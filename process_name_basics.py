import re


def change_column_names_to_snake_case(name_basics_df):
    """
    To change the column names to snake_case style.

    Args:
        name_basics_df (dataframe): The dataframe name_basics.

    Returns:
        dataframe with the new changed column names.

    """
    new_columns_names = [to_snake_case(col) for col in name_basics_df.columns]
    for idx, old_col in enumerate(name_basics_df.columns):
        name_basics_df = name_basics_df.withColumnRenamed(old_col, new_columns_names[idx])
    return name_basics_df


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
