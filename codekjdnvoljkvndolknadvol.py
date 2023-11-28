import os
import pandas as pd

# Set the path to the folder containing CSV files 
folder_path = ("C:/Users/no45a/OneDrive/Desktop/MQP/E1 term/summernsf20.1_Sum1/Fitbit")

dfs=[]
# Initialize an empty DataFrame to store the combined data 
#combined_data = pd.DataFrame() 
# Iterate through all files in the folder 
for filename in os.listdir(folder_path): 
    if filename.startswith("calories") and filename.endswith(".csv"): 
        # Read each CSV file into a DataFrame 
        file_path = os.path.join(folder_path, filename) 
        df = pd.read_csv(file_path) 
        # Concatenate the current DataFrame with the combined_data DataFrame 
        #combined_data = pd.concat([combined_data, df], ignore_index=True) 
        df=df.iloc[:,2:3].sum()
        dfs.append(df)
combined_data = pd.concat(dfs ,axis=1)
# Write the combined data to a new CSV file 
combined_data.to_csv('C:/Users/no45a/OneDrive/Desktop/MQP/combined_data.csv', index=False) 