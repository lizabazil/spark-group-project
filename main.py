from basic_dfs.basic_df_Rechkalova import basic_test_df as basic_test_df2
from basic_dfs.basic_df_Shvets import basic_test_df as basic_test_df3
from basic_dfs.basic_df_Tretiak import basic_test_df as basic_test_df1
from io_h import *
from setting import *
from process.process_title_basics import make_genres_array_type, convert_is_adult_col_to_boolean_type
from process.process_title_crew import convert_directors_col_to_array, convert_writers_col_to_array
from process.process_name_basics import (make_primary_profession_col_array_type,
                                         make_known_for_titles_col_array_type, create_age_col, create_is_alive_col,
                                         rename_nconst_col)
from process.common_functions import change_column_names_to_snake_case, null_from_string_to_none
from process.process_title_akas import (make_types_col_array_type, make_attribute_col_array_type,
                                        make_is_original_title_col_boolean_type)
from cleaning import drop_types_column, drop_attributes_column
import pyspark.sql.functions as F

# df2 = basic_test_df2()
# df2.show()

# df3 = basic_test_df3()
# df3.show()

# df1 = basic_test_df1()
# df1.show()


# df1_name_basics = read_name_basics_df(name_basics_path)  # liza's
# # df1_name_basics.show(truncate=False)  # remove before commit
# # write_name_basics_to_csv(df1_name_basics)
# # change column names to snake_case style
# snake_case_name_basics_df = change_column_names_to_snake_case(df1_name_basics)
# # change primary_profession type to array type
# modified_col_primary_profession_df = make_primary_profession_col_array_type(snake_case_name_basics_df)
# modified_col_known_for_titles_df = make_known_for_titles_col_array_type(modified_col_primary_profession_df)
#
# # create new column 'age'
# with_age_col_df = create_age_col(modified_col_known_for_titles_df)
# with_is_living_col_df = create_is_alive_col(with_age_col_df)
# renamed_col_nconst_df = rename_nconst_col(with_is_living_col_df)
# # renamed_col_nconst_df.show(truncate=False)
# write_dataframe_to_csv(renamed_col_nconst_df, name_basics_write_path)
#
# # title.basics.tsv
# title_basics_df = read_title_basics_df(title_basics_path)
# title_basics_to_snake_case_df = change_column_names_to_snake_case(title_basics_df)
# title_basics_genres_col_modified_df = make_genres_array_type(title_basics_to_snake_case_df)
#
# convert_is_adult_col_to_bool_df = convert_is_adult_col_to_boolean_type(title_basics_genres_col_modified_df)
# null_from_string_to_none_df = null_from_string_to_none(convert_is_adult_col_to_bool_df)
# # null_from_string_to_none_df.show(truncate=False)
#
# # write to the file
# write_dataframe_to_csv(null_from_string_to_none_df, title_basics_write_path)

df_title_akas = read_title_akas_df(title_akas_path)
df_snake_case_akas = change_column_names_to_snake_case(df_title_akas)
df_title_akas_without_n = null_from_string_to_none(df_snake_case_akas)
df_title_akas_types_array = make_types_col_array_type(df_title_akas_without_n)
df_title_akas_attributes_array = make_attribute_col_array_type(df_title_akas_types_array)
df_title_akas_is_original_title_boolean = make_is_original_title_col_boolean_type(df_title_akas_attributes_array)

# df_title_akas_is_original_title_boolean.show()
df_title_akas_drop_types = drop_types_column(df_title_akas_is_original_title_boolean)
df_title_akas_drop_attributes = drop_attributes_column(df_title_akas_drop_types)
df_title_akas_drop_attributes.show()

# write_dataframe_to_csv(df_title_akas_is_original_title_boolean, title_akas_write_path)
#
# df_episode = read_title_episode_df(title_episode_path)
# df_snake_case_episode = change_column_names_to_snake_case(df_episode)
# df_title_episode_without_n = null_from_string_to_none(df_snake_case_episode)
# # df_title_episode_without_n.show()
# write_dataframe_to_csv(df_title_episode_without_n, title_episode_write_path)

#
# title_principals_df = read_title_principals_df(title_principals_path)
# snake_case_title_principals_df = change_column_names_to_snake_case(title_principals_df)
# title_principals_df_with_nulls = null_from_string_to_none(snake_case_title_principals_df)
# write_dataframe_to_csv(title_principals_df_with_nulls, title_principal_write_path)
#
# title_crew_df = read_title_crew_df(title_crew_path)
# snake_case_title_crew_df = change_column_names_to_snake_case(title_crew_df)
# title_crew_df_with_nulls = null_from_string_to_none(snake_case_title_crew_df)
# title_crew_df_with_directors_as_array = convert_directors_col_to_array(title_crew_df_with_nulls)
# title_crew_df_with_writers_as_array = convert_writers_col_to_array(title_crew_df_with_directors_as_array)
# write_dataframe_to_csv(title_crew_df_with_writers_as_array, title_crew_write_path)
#
# title_ratings_df = read_title_ratings_df(title_ratings_path)
# snake_case_title_ratings_df = change_column_names_to_snake_case(title_ratings_df)
# write_dataframe_to_csv(snake_case_title_ratings_df, title_ratings_write_path)
