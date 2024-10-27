from columns import job, characters, directors, writers
import pyspark.sql.functions as f


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
