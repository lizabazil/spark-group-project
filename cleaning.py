from columns import job, characters


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
