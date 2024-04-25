import os
import pandas as pd
import numpy as np


def student(folder_path, output_file_path):
    """
    Runs all to get one student concatenated information on one feature

    :param folder_path: path of the term folder
    :param combined_data: data frame to put information in
    :return: dataframe containing concatenation of given feature for given student
    """

    output_directory = os.path.dirname(output_file_path)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    if os.path.exists(output_file_path):
        combined_data = pd.read_csv(output_file_path)
    else:
        combined_data = pd.DataFrame()
    # Iterate through all files in the folder
    for student_dir in os.listdir(folder_path):
        ID = student_dir
        student_dir_path = os.path.join(folder_path, student_dir)
        if os.path.isdir(student_dir_path):
            # Initialize an empty DataFrame for the current student
            student_data = {'Date': [], 'CESD': []}
            # Iterate through all Excel files in the Survey directory
            survey_path = os.path.join(student_dir_path, "Survey")
            for filename in os.listdir(survey_path):
                if filename.endswith(".csv"):
                    file_path = os.path.join(survey_path, filename)
                    # Read each Excel file into a DataFrame
                    df = pd.read_csv(file_path)

                    student_data['Date'].extend(df['StartDate'])
                    student_data['CESD'].extend(df['CESD'])
                    student_data['STAI'] = df.filter(like='STAI_St_').sum(axis=1)
                    student_data['ThermalSens'] = df.filter(like='ThermalSens').sum(axis=1)
                    student_data['TempSatisf'] = df.filter(like='TempSatisf').sum(axis=1)
                    student_data['ThermEnh/Int'] = df.filter(like='ThermEnh/Int').sum(axis=1)
                    student_data['AirSatisf_1'] = df.filter(like='AirSatisf_1').sum(axis=1)
                    student_data['OutdoorAirAmount_1'] = df.filter(like='OutdoorAirAmount_1').sum(axis=1)
                    student_data['AirEnh/Int_1'] = df.filter(like='AirEnh/Int_1').sum(axis=1)
                    student_data['LightSatisf_1'] = df.filter(like='LightSatisf_1').sum(axis=1)
                    student_data['LightSatisf_2'] = df.filter(like='LightSatisf_2').sum(axis=1)
                    student_data['LightAmount_1'] = df.filter(like='LightAmount_1').sum(axis=1)
                    student_data['LightEnh/Int_1'] = df.filter(like='LightEnh/Int_1').sum(axis=1)
                    student_data['NoiseSatisf_1'] = df.filter(like='NoiseSatisf_1').sum(axis=1)
                    student_data['NoiseEnh/Int_1'] = df.filter(like='NoiseEnh/Int_1').sum(axis=1)
                    student_data['EnvSatisf_1'] = df.filter(like='EnvSatisf_1').sum(axis=1)
                    student_data['LearnIncr/Decr_1'] = df.filter(like='LearnIncr/Decr_1').sum(axis=1)
                    student_data['CPUSpeed_1'] = df.filter(like='CPUSpeed_1').sum(axis=1)
                    student_data['InternetSpeed_1'] = df.filter(like='InternetSpeed_1').sum(axis=1)
                    student_data['DownloadSpeed'] = df.filter(like='DownloadSpeed').sum(axis=1)
                    student_data['UploadSpeed'] = df.filter(like='UploadSpeed').sum(axis=1)




            # Create a DataFrame for the current student
            student_df = pd.DataFrame(student_data)
            student_df = student_df.rename(columns={'CESD': "CESD" + "_" + ID})
            student_df['STAI'] = np.where(student_df['STAI'] == 0, np.nan, student_df['STAI'])
            student_df = student_df.rename(columns={'STAI': "STAI" + "_" + ID})
            student_df = student_df.rename(columns={'ThermalSens': "ThermalSens" + "_" + ID})
            student_df = student_df.rename(columns={'TempSatisf': "TempSatisf" + "_" + ID})
            student_df = student_df.rename(columns={'ThermEnh/Int': "ThermEnh/Int" + "_" + ID})
            student_df = student_df.rename(columns={'AirSatisf_1': "AirSatisf_1" + "_" + ID})
            student_df = student_df.rename(columns={'OutdoorAirAmount_1': "OutdoorAirAmount_1" + "_" + ID})
            student_df = student_df.rename(columns={'AirEnh/Int_1': "AirEnh/Int_1" + "_" + ID})
            student_df = student_df.rename(columns={'LightSatisf_1': "LightSatisf_1" + "_" + ID})
            student_df = student_df.rename(columns={'LightSatisf_2': "LightSatisf_2" + "_" + ID})
            student_df = student_df.rename(columns={'LightAmount_1': 'LightAmount_1' + '_' + ID})
            student_df = student_df.rename(columns={'LightEnh/Int_1': 'LightEnh/Int_1' + '_' + ID})
            student_df = student_df.rename(columns={'NoiseSatisf_1': 'NoiseSatisf_1' + '_' + ID})
            student_df = student_df.rename(columns={'NoiseEnh/Int_1': 'NoiseEnh/Int_1' + '_' + ID})
            student_df = student_df.rename(columns={'EnvSatisf_1': 'EnvSatisf_1' + '_' + ID})
            student_df = student_df.rename(columns={'LearnIncr/Decr_1': 'LearnIncr/Decr_1' + '_' + ID})
            student_df = student_df.rename(columns={'CPUSpeed_1': 'CPUSpeed_1' + '_' + ID})
            student_df = student_df.rename(columns={'InternetSpeed_1': 'InternetSpeed_1' + '_' + ID})
            student_df = student_df.rename(columns={'DownloadSpeed': 'DownloadSpeed' + '_' + ID})
            student_df = student_df.rename(columns={'UploadSpeed': 'UploadSpeed' + '_' + ID})


            # Merge the student DataFrame with the combined data on the 'Date' column
            if combined_data.empty:
                combined_data = student_df
            else:
                student_df['Date'] = pd.to_datetime(student_df['Date'])
                combined_data['Date'] = pd.to_datetime(combined_data['Date'])
                combined_data = pd.merge(combined_data, student_df, on='Date', how='outer',
                                         suffixes=('', f'_{student_dir}'))
    combined_data = combined_data.sort_values(by='Date')
    combined_data.to_csv(output_file_path, index=False)
