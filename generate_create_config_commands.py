import os 
import pandas as pd
pd.options.display.max_rows=100
pd.options.display.max_columns=100

df = pd.read_csv('core forecasting github migration.csv')
df = df[df['Github-Actions']=='TO DO'][['old_pipeline_name','new_pipeline_folder','new_pipeline_name']].reset_index(drop=True)
print(df)

print("*"*100)
for i in range(len(df)):
    x = df.iloc[i]
    
    command = f"python get-functions.py {x.new_pipeline_folder} {x.new_pipeline_name} {x.old_pipeline_name} conf"
    path = os.path.join(x.new_pipeline_folder,x.new_pipeline_name,x.new_pipeline_name)
    print(command)
    # print(f"directory {'exists' if os.path.isdir(path) else 'does not exist'} | folder: {path}")
    # if not os.path.isdir(path):
    #     os.makedirs(path)
    #     if os.path.isdir(path):
    #         print(f"Successfully created directories: {path}")
