from columns import types, attributes
import pyspark.sql.functions as f


def make_types_col_array_type(title_akas_df):
    """
    Change string column "types" to array type.
    from developer.imdb.com:
        "types (array) - Enumerated set of attributes for this alternative title.
        One or more of the following: "alternative", "dvd", "festival", "tv", "video",
        "working", "original", "imdbDisplay".
        New values may be added in the future without warning"

    Args:
        title_akas_df (dataframe): The dataframe.

    Returns:
        dataframe: The dataframe with array values.
    """
    title_akas_df = title_akas_df.withColumn(types,
                                             f.split(title_akas_df[types], ','))
    return title_akas_df


def make_attribute_col_array_type(title_akas_df):
    """
    Change string column "attribute" to array type.
    from developer.imdb.com:
        "attributes (array) - Additional terms to describe this alternative title, not enumerated"

    Args:
        title_akas_df (dataframe): The dataframe.

    Returns:
        dataframe: The dataframe with array values.
    """
    title_akas_df = title_akas_df.withColumn(attributes,
                                             f.split(title_akas_df[attributes], ','))
    return title_akas_df
