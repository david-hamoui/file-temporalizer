import pandas as pd

df = pd.read_parquet('C:/Users/t.david.hamoui/file-temporalizer/incidents.parquet')
sorted_df = df.sort_values(by='created_at')


print(sorted_df['created_at'])


sorted_df.to_parquet('myfile.parquet')