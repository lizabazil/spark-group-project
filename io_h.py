from spark_session import spark_session


def read_title_akas_df(path_to_df):
    """
    Read title.akas.tsv file and return
    :param path_to_df:
    :return: dataframe
    """
    spark = spark_session.getActiveSession()
    from_tsv_default_df = spark.read.csv(path_to_df, sep=r"\t")
    return from_tsv_default_df
