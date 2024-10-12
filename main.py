from basic_dfs.basic_df_Rechkalova import basic_test_df as basic_test_df2
from basic_dfs.basic_df_Shvets import basic_test_df as basic_test_df3
from basic_dfs.basic_df_Tretiak import basic_test_df as basic_test_df1
from setting import path
from io_h import read_name_basics_df

df2 = basic_test_df2()
df2.show()

df3 = basic_test_df3()
df3.show()

df1 = basic_test_df1()
df1.show()

df1_name_basics = read_name_basics_df(path)
# df1_name_basics.show(truncate=False)  # remove before commit
