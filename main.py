from basic_dfs.basic_df_Rechkalova import basic_test_df as basic_test_df2
from basic_dfs.basic_df_Shvets import basic_test_df as basic_test_df3
from basic_dfs.basic_df_Tretiak import basic_test_df as basic_test_df1
from io_h import *
from setting import *
from process.process_title_basics import make_genres_array_type, convert_is_adult_col_to_boolean_type
from process.process_title_crew import convert_directors_col_to_array, convert_writers_col_to_array
from process.process_name_basics import (make_primary_profession_col_array_type,
                                         make_known_for_titles_col_array_type,
                                         rename_nconst_col)
from process.common_functions import change_column_names_to_snake_case, null_from_string_to_none
from process.process_title_akas import (make_types_col_array_type, make_attribute_col_array_type,
                                        make_is_original_title_col_boolean_type)
from cleaning import *
from filtering.filtering_Shvets import *


def dealing_with_null_columns_name_basics(name_basics_df):
    """
    To perform operations on dealing with null columns in name.basics dataframe.

    Args:
        name_basics_df (pyspark dataframe): name.basiscs dataframe.

    Returns:
        (pyspark dataframe): name.basics dataframe with performed operations (dealing with null columns, such as
        deleting or filling them)
    """
    deleted_col_birth_year_df = drop_col_birth_year_from_name_basics_df(name_basics_df)
    deleted_col_death_year_df = drop_col_death_year_from_name_basics_df(deleted_col_birth_year_df)
    return deleted_col_death_year_df


def dealing_with_null_columns_title_basics(title_basics_dataframe):
    """
    To perform operations on dealing with null columns in title.basics dataframe.

    Args:
        title_basics_dataframe (pyspark dataframe): title.basiscs dataframe.

    Returns:
        (pyspark dataframe): title.basics dataframe with performed operations (dealing with null columns, such as
        deleting or filling them)
    """
    with_filled_col_end_year_df = fill_col_end_year_in_title_basics_df(title_basics_dataframe)
    with_filled_col_runtime_minutes_df = fill_col_runtime_minutes_in_title_basics(with_filled_col_end_year_df)
    return with_filled_col_runtime_minutes_df


def processing_cols_name_basics(name_basics_df):
    """
    To perform operations on processing columns in name.basics dataframe (such as changing columns types and names)

    Args:
         name_basics_df (pyspark dataframe): name.basics dataframe

    Returns:
        (pyspark dataframe): name.basics dataframe with performed operations
    """
    null_values_to_none_df = null_from_string_to_none(name_basics_df)
    snake_case_name_basics_df = change_column_names_to_snake_case(null_values_to_none_df)
    modified_col_primary_profession_df = make_primary_profession_col_array_type(snake_case_name_basics_df)
    modified_col_known_for_titles_df = make_known_for_titles_col_array_type(modified_col_primary_profession_df)

    renamed_col_nconst_df = rename_nconst_col(modified_col_known_for_titles_df)
    return renamed_col_nconst_df


def processing_cols_title_basics(title_basics_dataframe):
    """
    To perform operations on processing columns in title.basics dataframe (such as changing columns types and names)

    Args:
        title_basics_dataframe (pyspark dataframe): title.basics dataframe

    Returns:
        (pyspark dataframe): title.basics dataframe with performed operations
       """
    null_values_to_none_df = null_from_string_to_none(title_basics_dataframe)
    title_basics_to_snake_case_df = change_column_names_to_snake_case(null_values_to_none_df)
    title_basics_genres_col_modified_df = make_genres_array_type(title_basics_to_snake_case_df)

    convert_is_adult_col_to_bool_df = convert_is_adult_col_to_boolean_type(title_basics_genres_col_modified_df)
    return convert_is_adult_col_to_bool_df


def process_cols_title_principals(title_principals):
    """
    Processes title_principals dataframe (change to snake case, types)
    Args:
        title_principals (dataframe): title_principals df
    Returns:
        (dataframe): processed title_principals df
    """
    snake_case_title_principals_df = change_column_names_to_snake_case(title_principals)
    title_principals_df_with_nulls = null_from_string_to_none(snake_case_title_principals_df)
    return title_principals_df_with_nulls


def clean_title_principals(title_principals):
    """
    Cleans title_principals dataframe (null values and duplicates)
    Args:
        title_principals (dataframe): title_principals df
    Returns:
        (dataframe): title_principals df with processed null values
    """
    title_principals_df_without_job_col = drop_job_column_in_title_principals(title_principals)
    title_principals_df_without_characters = drop_characters_column_in_title_principals(
        title_principals_df_without_job_col)
    title_principals_df_without_duplicates = delete_duplicates(title_principals_df_without_characters)
    return title_principals_df_without_duplicates


def process_cols_title_crew(title_crew):
    """
    Processes title_crew dataframe (change to snake case, types)
    Args:
        title_crew (dataframe): title_crew df
    Returns:
        (dataframe): processed title_crew df
    """
    snake_case_title_crew_df = change_column_names_to_snake_case(title_crew)
    title_crew_df_with_nulls = null_from_string_to_none(snake_case_title_crew_df)
    title_crew_df_with_directors_as_array = convert_directors_col_to_array(title_crew_df_with_nulls)
    title_crew_df_with_writers_as_array = convert_writers_col_to_array(title_crew_df_with_directors_as_array)
    return title_crew_df_with_writers_as_array


def clean_title_crew(title_crew):
    """
    Cleans title_crew dataframe (null values and duplicates)
    Args:
        title_crew (dataframe): title_crew df
    Returns:
        (dataframe): title_crew df with processed null values
    """
    title_crew_df_without_null_rows = drop_null_rows_in_title_crew(title_crew)
    title_crew_df_without_duplicates = delete_duplicates(title_crew_df_without_null_rows)
    return title_crew_df_without_duplicates


def business_questions_shvets(name_basics, title_basics):
    """
    Finds answers to business questions 18-20 and writes results to csv files.
    Args:
        name_basics (dataframe): name_basics dataframe
        title_basics (dataframe): title_basics dataframe
    Returns:
        None
    """
    directors_not_producers = get_directors_not_producers(name_basics)
    write_name_basics_to_csv(directors_not_producers, 'data/results/question_18')

    people_with_only_2_professions = get_people_with_only_2_professions(name_basics)
    write_name_basics_to_csv(people_with_only_2_professions, 'data/results/question_19')

    dramas_with_more_than_70_mins_runtime = get_dramas_with_more_than_70_mins_runtime(title_basics)
    write_title_basics_to_csv(dramas_with_more_than_70_mins_runtime, 'data/results/question_20')
    return None


# df2 = basic_test_df2()
# df2.show()

# df3 = basic_test_df3()
# df3.show()

# df1 = basic_test_df1()
# df1.show()

# name.basics.tsv
df1_name_basics = read_name_basics_df(name_basics_path)
after_processing_name_basics_df = processing_cols_name_basics(df1_name_basics)

after_dealing_with_null_cols_df = dealing_with_null_columns_name_basics(after_processing_name_basics_df)
name_basics_df_without_duplicates = delete_duplicates(after_dealing_with_null_cols_df)
write_name_basics_to_csv(name_basics_df_without_duplicates, name_basics_write_path)

# title.basics.tsv
title_basics_df = read_title_basics_df(title_basics_path)

after_processing_title_basics_df = processing_cols_title_basics(title_basics_df)
after_dealing_with_null_cols_title_basics_df = dealing_with_null_columns_title_basics(after_processing_title_basics_df)
title_basics_df_without_duplicates = delete_duplicates(after_dealing_with_null_cols_title_basics_df)
write_title_basics_to_csv(title_basics_df_without_duplicates, title_basics_write_path)

df_title_akas = read_title_akas_df(title_akas_path)
df_snake_case_akas = change_column_names_to_snake_case(df_title_akas)
df_title_akas_without_n = null_from_string_to_none(df_snake_case_akas)
df_title_akas_types_array = make_types_col_array_type(df_title_akas_without_n)
df_title_akas_attributes_array = make_attribute_col_array_type(df_title_akas_types_array)
df_title_akas_is_original_title_boolean = make_is_original_title_col_boolean_type(df_title_akas_attributes_array)
df_title_akas_drop_types = drop_types_column(df_title_akas_is_original_title_boolean)
df_title_akas_drop_attributes = drop_attributes_column(df_title_akas_drop_types)
title_akas_fillna_region_language = fillna_region_language_with_unknown(df_title_akas_drop_attributes)
title_akas_without_duplicates = delete_duplicates(title_akas_fillna_region_language)
# title_akas_fillna_region_language.show()
write_dataframe_to_csv(title_akas_without_duplicates, title_akas_write_path)

df_episode = read_title_episode_df(title_episode_path)
df_snake_case_episode = change_column_names_to_snake_case(df_episode)
df_title_episode_without_n = null_from_string_to_none(df_snake_case_episode)
df_episode_without_null_rows = drop_null_rows_episode(df_title_episode_without_n)
df_episode_without_duplicates = delete_duplicates(df_episode_without_null_rows)
# df_episode_without_null_rows.show()
write_dataframe_to_csv(df_episode_without_duplicates, title_episode_write_path)


title_principals_df = read_title_principals_df(title_principals_path)
processed_title_principals_df = process_cols_title_principals(title_principals_df)
cleaned_title_principals_df = clean_title_principals(processed_title_principals_df)
write_dataframe_to_csv(cleaned_title_principals_df, title_principal_write_path)

title_crew_df = read_title_crew_df(title_crew_path)
processed_title_crew = process_cols_title_crew(title_crew_df)
cleaned_title_crew = clean_title_crew(processed_title_crew)
write_title_crew_to_csv(cleaned_title_crew, title_crew_write_path)

title_ratings_df = read_title_ratings_df(title_ratings_path)
snake_case_title_ratings_df = change_column_names_to_snake_case(title_ratings_df)
title_ratings_df_without_duplicates = delete_duplicates(snake_case_title_ratings_df)
write_dataframe_to_csv(title_ratings_df_without_duplicates, title_ratings_write_path)

business_questions_shvets(name_basics_df_without_duplicates, title_basics_df_without_duplicates)
