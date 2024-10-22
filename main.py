from basic_dfs.basic_df_Rechkalova import basic_test_df as basic_test_df2
from basic_dfs.basic_df_Shvets import basic_test_df as basic_test_df3
from basic_dfs.basic_df_Tretiak import basic_test_df as basic_test_df1
from io_h import read_name_basics_df, write_name_basics_to_csv, read_title_crew_df, write_title_crew_df_to_csv
from io_h import (read_title_principals_df, write_title_principals_df_to_csv, read_title_akas_df,
                  write_title_akas_df_to_csv, read_title_episode_df, write_title_episode_df_to_csv)
from setting import path
from process.process_name_basics import (make_primary_profession_col_array_type,
                                         make_known_for_titles_col_array_type, create_age_col, create_is_alive_col,
                                         rename_nconst_col)
from process.common_functions import change_column_names_to_snake_case, null_from_string_to_none


df2 = basic_test_df2()
df2.show()

df3 = basic_test_df3()
df3.show()

df1 = basic_test_df1()
df1.show()


df1_name_basics = read_name_basics_df(path)  # liza's
# df1_name_basics.show(truncate=False)  # remove before commit
# write_name_basics_to_csv(df1_name_basics)
# change column names to snake_case style
snake_case_name_basics_df = change_column_names_to_snake_case(df1_name_basics)
# change primary_profession type to array type
modified_col_primary_profession_df = make_primary_profession_col_array_type(snake_case_name_basics_df)
modified_col_known_for_titles_df = make_known_for_titles_col_array_type(modified_col_primary_profession_df)

# create new column 'age'
with_age_col_df = create_age_col(modified_col_known_for_titles_df)
with_is_living_col_df = create_is_alive_col(with_age_col_df)
renamed_col_nconst_df = rename_nconst_col(with_is_living_col_df)
# renamed_col_nconst_df.show(truncate=False)
write_name_basics_to_csv(renamed_col_nconst_df)

df_title_akas = read_title_akas_df(path)
df_snake_case_akas = change_column_names_to_snake_case(df_title_akas)
df_title_akas_without_n = null_from_string_to_none(df_snake_case_akas)
# df_title_akas_without_n.show()
write_title_akas_df_to_csv(df_title_akas_without_n)

df_episode = read_title_episode_df(path)
df_snake_case_episode = change_column_names_to_snake_case(df_episode)
df_title_episode_without_n = null_from_string_to_none(df_snake_case_episode)
# df_title_episode_without_n.show()
write_title_episode_df_to_csv(df_title_episode_without_n)

title_principals_df = read_title_principals_df(path)
snake_case_title_principals_df = change_column_names_to_snake_case(title_principals_df)
title_principals_df_with_nulls = null_from_string_to_none(snake_case_title_principals_df)
write_title_principals_df_to_csv(title_principals_df_with_nulls)

title_crew_df = read_title_crew_df(path)
snake_case_title_crew_df = change_column_names_to_snake_case(title_crew_df)
write_title_crew_df_to_csv(snake_case_title_crew_df)
