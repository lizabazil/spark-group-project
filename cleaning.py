# title_akas
from columns import types, attributes


def drop_types_column(df):
    df = df.drop(types)
    return df


