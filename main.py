from basic_dfs.basic_df_Rechkalova import basic_test_df as basic_test_df2
from basic_dfs.basic_df_Shvets import basic_test_df as basic_test_df3
from basic_dfs.basic_df_Tretiak import basic_test_df as basic_test_df1
from io_h import read_name_basics_df, write_name_basics_to_csv
from io_h import read_title_principals_df, write_title_principals_df_to_csv, read_title_akas_df, write_title_akas_df_to_csv
from setting import path


df2 = basic_test_df2()
df2.show()

df3 = basic_test_df3()
df3.show()

df1 = basic_test_df1()
df1.show()


df1_name_basics = read_name_basics_df(path)  # liza's
# df1_name_basics.show(truncate=False)  # remove before commit
write_name_basics_to_csv(df1_name_basics)

df_title_akas = read_title_akas_df(path)
write_title_akas_df_to_csv(df_title_akas)

df3_title_principals = read_title_principals_df(path)
write_title_principals_df_to_csv(df3_title_principals)
