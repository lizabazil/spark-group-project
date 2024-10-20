import re
import pyspark.sql.functions as f


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


def make_primary_profession_col_array_type(name_basics_df):
    """
    To modify the column  'primary_profession' type to array type.

    Args:
        name_basics_df (pyspark dataframe): The name_basics dataframe.

    Returns:
        pyspark dataframe: dataframe with the modified column type 'primary_profession'.
    """
    primary_profession_col_name = 'primary_profession'
    name_basics_df = name_basics_df.withColumn(primary_profession_col_name,
                                               f.split(name_basics_df[primary_profession_col_name], ','))
    return name_basics_df


def make_known_for_titles_col_array_type(name_basics_df):
    """
    To modify the column  'known_for_titles' type to array type.

    Args:
        name_basics_df (pyspark dataframe): The name_basics dataframe.

    Returns:
        pyspark dataframe: dataframe with the modified column type 'known_for_titles'.
    """
    known_for_titles_col_name = 'known_for_titles'
    name_basics_df = name_basics_df.withColumn(known_for_titles_col_name,
                                               f.split(name_basics_df[known_for_titles_col_name], ','))
    return name_basics_df


def create_age_col(name_basics_df):
    """
    To create new column with age of person.

    Args:
         name_basics_df (pyspark dataframe): The name_basics dataframe.

    Returns:
        pyspark dataframe: dataframe with the added column 'age' base on birth_year and death_year.
    """
    birth_year_col_name = 'birth_year'
    death_year_col_name = 'death_year'
    name_basics_df = name_basics_df.withColumn('age', f.when(f.col(death_year_col_name).isNull(), f.year(f.current_date()) -
                                                             f.col(birth_year_col_name))
                                               .otherwise(f.col(death_year_col_name) - f.col(birth_year_col_name)))
    return name_basics_df
