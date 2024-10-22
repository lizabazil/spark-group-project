from basic_dfs.basic_df_Rechkalova import basic_test_df as basic_test_df2
from basic_dfs.basic_df_Shvets import basic_test_df as basic_test_df3
from basic_dfs.basic_df_Tretiak import basic_test_df as basic_test_df1
from io_h import read_name_basics_df, write_name_basics_to_csv
from io_h import (read_title_principals_df, write_title_principals_df_to_csv, read_title_akas_df,
                  write_title_akas_df_to_csv, read_title_episode_df, write_title_episode_df_to_csv)
from setting import path
from process.process_name_basics import (make_primary_profession_col_array_type,
                                         make_known_for_titles_col_array_type, create_age_col, create_is_alive_col,
                                         rename_nconst_col)
from process.common_functions import change_column_names_to_snake_case, null_from_string_to_none
from process.process_title_akas import (make_types_col_array_type, make_attribute_col_array_type,
                                        make_is_original_title_col_boolean_type)


# df2 = basic_test_df2()
# df2.show()
#
# df3 = basic_test_df3()
# df3.show()
#
# df1 = basic_test_df1()
# df1.show()

#
# df1_name_basics = read_name_basics_df(path)  # liza's
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
# write_name_basics_to_csv(renamed_col_nconst_df)

df_title_akas = read_title_akas_df(path)
df_snake_case_akas = change_column_names_to_snake_case(df_title_akas)
df_title_akas_without_n = null_from_string_to_none(df_snake_case_akas)
df_title_akas_types_array = make_types_col_array_type(df_title_akas_without_n)
df_title_akas_attributes_array = make_attribute_col_array_type(df_title_akas_types_array)
df_title_akas_is_original_title_boolean = make_is_original_title_col_boolean_type(df_title_akas_attributes_array)
# df_title_akas_is_original_title_boolean.show()
write_title_akas_df_to_csv(df_title_akas_is_original_title_boolean)

# df_episode = read_title_episode_df(path)
# df_snake_case_episode = change_column_names_to_snake_case(df_episode)
# df_title_episode_without_n = null_from_string_to_none(df_snake_case_episode)
# # df_title_episode_without_n.show()
# write_title_episode_df_to_csv(df_title_episode_without_n)
#
# df3_title_principals = read_title_principals_df(path)
# write_title_principals_df_to_csv(df3_title_principals)
