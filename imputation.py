import pandas as pd
import numpy as np

# Get the columns that start with "CESD"
df = pd.read_csv("/Users/oliviaraisbeck/Downloads/MQP/all.csv") #change to your file location

#SURVEY IMPUTATION
cesd_columns = [col for col in df.columns if col.startswith('CESD')]
stai_columns = [col for col in df.columns if col.startswith('STAI')]

# Apply forward fill for each CESD column
for cesd_column in cesd_columns:
    df[cesd_column] = df[cesd_column].fillna(method='bfill')
    mask = ~df[cesd_column].isnull()
    #df[cesd_column] = np.where(df[cesd_column] < 10, 0, 1)

for stai_column in stai_columns:
    df[stai_column] = df[stai_column].fillna(method='bfill')
    mask = ~df[stai_column].isnull()
    #df[stai_column] = np.where(df[stai_column] < 40, 0, 1)

prefixes = ['ThermalSens', 'TempSatisf', 'ThermEnh/Int', 'AirSatisf_1', 'OutdoorAirAmount_1', 'AirEnh/Int_1', 'LightSatisf_1', 'LightSatisf_2', 'LightAmount_1', 'LightEnh/Int_1', 'NoiseSatisf_1', 'NoiseEnh/Int_1', 'EnvSatisf_1', 'LearnIncr/Decr_1', 'CPUSpeed_1', 'InternetSpeed_1', 'DownloadSpeed', 'UploadSpeed']

survey_columns = [col for col in df.columns if any(col.startswith(prefix) for prefix in prefixes)]
for col in survey_columns:
    df[col] = df[col].fillna(method='bfill')

#TAKING OUT DATA
step_columns = [col for col in df.columns if col.startswith('step_')]

# Iterate through each 'step_' column
for step_col in step_columns:
    # Find rows where the 'step_' column is 0
    zero_step_rows = df[df[step_col] < 250].index

    # Set 'step_' and the next 4 columns to NaN for those rows
    for row in zero_step_rows:
        df.loc[row, step_col:df.columns[df.columns.get_loc(step_col) + 4]] = np.nan

columns_to_impute = df.columns[df.columns.str.startswith(('step', 'heart', 'distance', 'sleep', 'calories'))]

# Iterate through each column and impute NaN values with the average of that column
for col in columns_to_impute:
    # Find indices of non-NaN values in the column
    non_nan_rows = df[col].notna()

    # Find the first and last non-NaN index in the column
    first_non_nan = non_nan_rows.idxmax()
    last_non_nan = non_nan_rows[::-1].idxmax()

    col_avg = df[col].mean()

    # If there are NaN values in the column, impute only within the range
    if first_non_nan and last_non_nan:
        df.loc[first_non_nan:last_non_nan, col] = df.loc[first_non_nan:last_non_nan, col].fillna(col_avg)

    #df[col] = df[col] / col_avg

df.to_csv("/Users/oliviaraisbeck/Downloads/MQP/filled.csv")

#REFORMAT INTO 6 ROWS
new_df = pd.DataFrame()

# Get the unique student IDs
#unique_student_ids = df.columns.str.extract(r'(\d+\.\d+)').dropna().squeeze().unique()
unique_student_ids = df.columns.str.extract(r'([^_]+)$').dropna()[0].unique()


# Iterate through each student and combine the data
for student_id in unique_student_ids:
    if(student_id == 'Date'):
        continue
    columns_with_id = df.columns[df.columns.str.contains(student_id)]
    student_data = {
        'ID': student_id,
        'steps': df[columns_with_id[::25]].values.flatten(),
        'heart': df[columns_with_id[1::25]].values.flatten(),
        'distance': df[columns_with_id[2::25]].values.flatten(),
        'sleep': df[columns_with_id[3::25]].values.flatten(),
        'calories': df[columns_with_id[4::25]].values.flatten(),
        'ThermalSens' : df[columns_with_id[7::25]].values.flatten(),
        'TempSatisf': df[columns_with_id[8::25]].values.flatten(),
        'ThermEnh/Int': df[columns_with_id[9::25]].values.flatten(),
        'AirSatisf_1': df[columns_with_id[10::25]].values.flatten(),
        'OutdoorAirAmount_1': df[columns_with_id[11::25]].values.flatten(),
        'AirEnh/Int_1' : df[columns_with_id[12::25]].values.flatten(),
        'LightSatisf_1': df[columns_with_id[13::25]].values.flatten(),
        'LightSatisf_2': df[columns_with_id[14::25]].values.flatten(),
        'LightAmount_1': df[columns_with_id[15::25]].values.flatten(),
        'LightEnh/Int_1': df[columns_with_id[16::25]].values.flatten(),
        'NoiseSatisf_1': df[columns_with_id[17::25]].values.flatten(),
        'NoiseEnh/Int_1': df[columns_with_id[18::25]].values.flatten(),
        'EnvSatisf_1': df[columns_with_id[19::25]].values.flatten(),
        'LearnIncr/Decr_1': df[columns_with_id[20::25]].values.flatten(),
        'CPUSpeed_1': df[columns_with_id[21::25]].values.flatten(),
        'InternetSpeed_1': df[columns_with_id[22::25]].values.flatten(),
        'DownloadSpeed': df[columns_with_id[23::25]].values.flatten(),
        'UploadSpeed': df[columns_with_id[24::25]].values.flatten(),
        'CESD': df[columns_with_id[5::25]].values.flatten(),
        'STAI': df[columns_with_id[6::25]].values.flatten(),
    }

    # Creating a temporary data frame for each student and appending to the main data frame
    student_df = pd.DataFrame(student_data)
    new_df = pd.concat([new_df, student_df], ignore_index=True)

    new_df = new_df.dropna(subset=['steps'])

new_df.to_csv("/Users/oliviaraisbeck/Downloads/MQP/filled_6.csv") #change to your file location