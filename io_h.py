from spark_session import spark_session


def read_name_basics_df(path_to_df):
    """
    Read dataset name.basics
    :param path_to_df:
    :return: dataframe
    """
    spark = spark_session.getActiveSession()
    df = spark.read.csv(path_to_df, sep=r'\t')
    return df
