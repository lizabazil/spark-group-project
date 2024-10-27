from columns import job


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
