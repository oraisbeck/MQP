import os
import pandas as pd
import numpy as np
from scipy.stats import mode
import openpyxl

# Configured set options of pandas to display more rows and columns of the dataset in the terminal
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def set_time_and_index(dataframe):
    """
    Set the index of a data frame as the 'Time'

    :param dataframe: a data frame to set the time and index of
    :return: data frame where its index is set to its 'Time'
    """
    dataframe['Time'] = pd.to_datetime(dataframe['Time'])
    dataframe.set_index(dataframe['Time'], inplace=True)
    return dataframe


def calorie_set_up(df_calories_concat, sample_type):
    """
    Takes calories time series data and resamples it by a
    specific sample typ(ex: hour, day, week, month)
    by calculating the sum for calories burned, and mode
    for calorie state

    :param df_calories_concat: calories time series data
    :param sample_type: the type of sampling: (ex: hour, day, week, month)
    :return: a new data frame containing the sampled time series data
    """
    if df_calories_concat.empty:
        return df_calories_concat
    else:
        updated_df = set_time_and_index(df_calories_concat)
        updated_df['Calories'] = pd.to_numeric(updated_df['Calories'], errors='coerce')

        up_calories_sum_sample = updated_df['Calories'].resample(sample_type).sum()
        up_calories_activity_state = updated_df['State'].resample(sample_type).apply(
            lambda x: x.mode().iloc[0] if not x.empty else None)
        up_calories_activity_interpreted = updated_df['Interpreted'].resample(sample_type).apply(
            lambda x: x.mode().iloc[0] if not x.empty else None)

        processed_cal_df = pd.DataFrame(
            {'Calories (Sum)': up_calories_sum_sample, 'Cal_State': up_calories_activity_state,
             'Cal_State_Interpreted': up_calories_activity_interpreted})
        return processed_cal_df


def heart_set_up(df_heart_concat, sample_type):
    """
    Takes heart time series data and resamples it by a
    specific sample type (ex: hour, day, week, month)
    by calculating the mean for heart rate (bpm)

    :param df_heart_concat: heart time series data
    :param sample_type: the type of sampling: (ex: hour, day, week, month)
    :return: a new data frame containing the sampled time series data
    """
    if df_heart_concat.empty:
        return df_heart_concat
    else:
        updated_heart_df = set_time_and_index(df_heart_concat)
        updated_heart_df['Heart Rate'] = pd.to_numeric(updated_heart_df['Heart Rate'], errors='coerce')
        processed_heart_df = pd.DataFrame(
            {'Heart Rate (Mean)': updated_heart_df['Heart Rate'].resample(sample_type).mean()})
        return processed_heart_df


def distance_set_up(df_distance_concat, sample_type):
    """
    Takes distance time series data and resamples it by a
    specific sample type (ex: hour, day, week, month)
    by calculating the sum for distance traveled

    :param df_distance_concat: distance time series data
    :param sample_type: the type of sampling: (ex: hour, day, week, month)
    :return: a new data frame containing the sampled time series data
    """
    if df_distance_concat.empty:
        return df_distance_concat
    else:
        updated_distance_df = set_time_and_index(df_distance_concat)
        processed_distance_df = pd.DataFrame(
            {'Distance (Sum)': updated_distance_df['Distance'].resample(sample_type).sum()})
        return processed_distance_df


def step_set_up(df_step_concat, sample_size):
    """
    Takes step time series data and resamples it by a
    specific sample type (ex: hour, day, week, month)
    by calculating the sum for total steps traveled

    :param df_step_concat: steps taken time series data
    :param sample_size: the type of sampling: (ex: hour, day, week, month)
    :return: a new data frame containing the sampled time series data
    """
    if df_step_concat.empty:
        return df_step_concat
    else:
        updated_step_df = set_time_and_index(df_step_concat)
        processed_step_df = pd.DataFrame({'Step (Sum)': updated_step_df['Step'].resample(sample_size).sum()})
        return processed_step_df


def sleep_set_up(df_sleep_concat, sample_size):
    """
    Takes sleep time series data and resamples it by a
    specific sample type (ex: hour, day, week, month)
    by calculating the mode for sleep state

    :param df_sleep_concat: sleep time series data
    :param sample_size: the type of sampling: (ex: hour, day, week, month)
    :return: a new data frame containing the sampled time series data
    """
    if df_sleep_concat.empty:
        return df_sleep_concat
    else:
        updated_sleep_df = set_time_and_index(df_sleep_concat)
        updated_sleep_df_state = updated_sleep_df['State'].resample(sample_size).apply(
            lambda x: x.mode().iloc[0] if not x.empty else None)
        updated_sleep_df_interp = updated_sleep_df['Interpreted'].resample(sample_size).apply(
            lambda x: x.mode().iloc[0] if not x.empty else None)
        processed_sleep_df = pd.DataFrame(
            {'Sleep_State': updated_sleep_df_state, 'Sleep_State_Interpreted': updated_sleep_df_interp})
        return processed_sleep_df


def survey_data_set_up(survey_excel_file_path):
    """
    Takes specific columns from survey data frame provided by the survey_excel_file_path

    :param survey_excel_file_path: file path to survey excel data sheet
    :return: new data frame containing only those specific columns
    """
    survey_df = pd.read_excel(survey_excel_file_path)
    up_survey_df = survey_df[['StartDate', 'ID', 'CESD']]
    up_survey_df = up_survey_df.rename(columns={'StartDate': 'Time'})
    up_df_survey_df = set_time_and_index(up_survey_df)
    processed_survey_df = pd.DataFrame({'Student ID': up_df_survey_df['ID'], 'CESD': up_df_survey_df['CESD']})
    return processed_survey_df


def extract_survey_excel_path(folder_path_to_survey):
    """
    Gets the path address and survey file name from folder_path_to_survey

    :param folder_path_to_survey: file path to survey
    :return: file path address, and name of Excel survey file
    """
    survey_files = os.listdir(folder_path_to_survey)
    survey_excel_files = [file for file in survey_files if file.endswith(".xlsx")]
    excel_file_name = survey_excel_files[0]
    survey_file_path = os.path.join(folder_path_to_survey, excel_file_name)
    return survey_file_path, excel_file_name


def concat_student_processed_data(file_path_to_term):
    """
    Concatenates the processed data for each student in
    "file_path_to_term" into one file to represent a term of sampled processed data

    Note: This function is to be called after the data has been
    extrapolated using this function: extrapolate_data_by_term

    :param file_path_to_term: directory path to term data
    :return: new csv file containing the term's processed
    data inside the "file_path_to_term" directory
    """
    list_of_df_to_concat = []

    for folder_name in os.listdir(file_path_to_term):
        if os.path.isdir(file_path_to_term + folder_name):
            if os.path.exists(file_path_to_term + folder_name + "/12H_processed_" + folder_name + ".csv"):
                student_df = pd.read_csv(file_path_to_term + folder_name + "/12H_processed_" + folder_name + ".csv")
                list_of_df_to_concat.append(student_df)

    concat_all_students = pd.concat(list_of_df_to_concat, ignore_index=False)
    final_df = set_time_and_index(concat_all_students)
    final_df_df = final_df[
        ['Student ID', 'Heart Rate (Mean)', 'Heart Rate Mask', 'Calories (Sum)', 'Calories Burned Mask',
         'Cal_State', 'Cal_State_Interpreted', 'Distance (Sum)', 'Distance Traveled Mask', 'Step (Sum)',
         'Steps Taken Mask', 'Sleep_State', 'Sleep_State_Interpreted', 'CESD', 'CESD Score Mask']].sort_values(
        by=['Student ID', 'Time'])
    final_df_df.to_csv(file_path_to_term + "12H_term_processed_data.csv")


def add_cesd_term_labels_pd(term):
    """
    Updates the term_processed_data.csv of specific term by adding columns for
    'CESD State', 'CESD State Interpreted', and 'Term' to the data frame

    :param term: name of desired term (ex: "E1 Term", "E2 Term", etc)
    :return: void
    """
    up_df = pd.read_csv(f"./{term}/12H_term_processed_data.csv")
    up_df['CESD State'] = up_df['CESD'].apply(lambda x: assign_state(x))
    up_df['CESD State Interpreted'] = up_df['CESD State'].apply(lambda x: assign_interpretation(x))

    final_df = set_time_and_index(up_df)
    final_df['Term'] = term
    final_df_df = final_df[['Student ID', 'Term', 'Heart Rate (Mean)', 'Heart Rate Mask', 'Calories (Sum)',
                            'Calories Burned Mask',
                            'Cal_State', 'Cal_State_Interpreted', 'Distance (Sum)', 'Distance Traveled Mask',
                            'Step (Sum)',
                            'Steps Taken Mask',
                            'Sleep_State', 'Sleep_State_Interpreted', 'CESD', 'CESD Score Mask', 'CESD State',
                            'CESD State Interpreted']].sort_values(by=['Student ID', 'Time'])
    final_df_df.to_csv(f"./{term}/12H_term_processed_data.csv")


def assign_state(value):
    """
    Takes a numerical CESD value for depression, and returns
    either 'Depressed' or 'Not Depressed' based on a certain
    threshold

    :param value: Depression value
    :return: string label for depression indication
    """
    if value >= 10:
        return 'Depressed'
    else:
        return 'Not Depressed'


def assign_interpretation(depression_label):
    """
    Assigns to numerical values to represent state of depression
    :param depression_label: string label
    :return: 1 for 'Depressed', otherwise 0
    """
    if depression_label == 'Depressed':
        return 1
    else:
        return 0


def create_label_df_from_processed_data(file_path_to_term, term):
    """
    Creates a new csv file containing the mean depression scores for each student
    found in the "file_path_to_term" directory

    :param file_path_to_term: directory path to term
    :param term: Name of the term
    :return: a new csv file containing the mean depression scores for term
    """
    term_data_df = pd.read_csv(file_path_to_term + "term_processed_data.csv")
    filtered_term_data_df = term_data_df[['Student ID', 'CESD']].dropna()
    grouped_data = filtered_term_data_df.groupby('Student ID')['CESD'].unique()
    mean_grouped_data = grouped_data.apply(lambda x: x.mean())
    final_label_df = pd.DataFrame({'CESD (Mean) Label': mean_grouped_data})
    final_label_df['CESD State'] = final_label_df['CESD (Mean) Label'].apply(lambda x: assign_state(x))
    final_label_df['CESD State Interpreted'] = final_label_df['CESD State'].apply(lambda x: assign_interpretation(x))
    final_label_df['Term'] = term
    final_label_df_up = final_label_df[['Term', 'CESD (Mean) Label', 'CESD State', 'CESD State Interpreted']]
    final_label_df_up.to_csv(f"./{term}/term_student_labels.csv")


def multi_month_file_concat(list_of_feature_arr, path_to_fitbit_data, participant_name, feature_file_type,
                            concat_file_start, takeaway_columns):
    """
    :param list_of_feature_arr: array containing multiple data frames of specific feature (ex: calories)
    :param path_to_fitbit_data: path containing the fitbit data
    :param participant_name: name of student (ex: summernsf20.23)
    :param feature_file_type: feature type (ex: calories, heart, step, distances, and sleep)
    :param concat_file_start: concat_file_start should be "cal" for calories, "heart" for heart
    "distance" for distance, "sleep" for sleep, and "step" for step
    :param takeaway_columns: array of columns to be included in the data frame

    :return: creates a new folder containing a single concatenated file for the specific feature type
    """
    # Make directories
    os.makedirs(
        path_to_fitbit_data + f"{feature_file_type}_{participant_name}/{concat_file_start}_concat_{participant_name}./")
    # upload feature csv
    if not (len(list_of_feature_arr) == 0):
        feature_concat = pd.concat(list_of_feature_arr)
        feature_concat_up = set_time_and_index(feature_concat)
        feature_concat_up.sort_index()
        feature_concat_up_final = feature_concat_up[takeaway_columns]
        feature_concat_up_final.to_csv(
            path_to_fitbit_data + f"{feature_file_type}_{participant_name}/{concat_file_start}_concat_{participant_name}./{concat_file_start}_concat_{participant_name}.csv")


def multi_month_data_pp(path_to_fitbit_data, participant_name):
    """
    Creates single concat feature folders and files
    for participants whose data is grouped by month and already been
    preprocessed for each month individually

    Note: This function is expected to be called after a participant, whose data is
    grouped by different months, has been preprocessed for those individual months

    "preprocessed" meaning their csv files for each feature type has been grouped,
    updated date and time, and concatenated into a single file

    :param path_to_fitbit_data: address path containing fitbit data
    :param participant_name: name of student (ex: summernsf20.23)
    :return: void
    """

    list_of_cal_df = []
    list_of_distance_df = []
    list_of_heart_df = []
    list_of_sleep_df = []
    list_of_step_df = []

    for folder in os.listdir(path_to_fitbit_data):
        if os.path.isdir(path_to_fitbit_data + folder):
            path_to_month_data = path_to_fitbit_data + folder
            for folder_nm in os.listdir(path_to_month_data):
                if "cal" in folder_nm:
                    if os.path.exists(
                            path_to_month_data + "/" + folder_nm + f"/cal_concat_{participant_name}." + f"/cal_concat_{participant_name}.csv"):
                        cal_df = pd.read_csv(
                            path_to_month_data + "/" + folder_nm + f"/cal_concat_{participant_name}." + f"/cal_concat_{participant_name}.csv")
                        list_of_cal_df.append(cal_df)
                elif "heart" in folder_nm:
                    if os.path.exists(
                            path_to_month_data + "/" + folder_nm + f"/heart_concat_{participant_name}." + f"/heart_concat_{participant_name}.csv"):
                        heart_df = pd.read_csv(
                            path_to_month_data + "/" + folder_nm + f"/heart_concat_{participant_name}." + f"/heart_concat_{participant_name}.csv")
                        list_of_heart_df.append(heart_df)
                elif "distance" in folder_nm:
                    if os.path.exists(
                            path_to_month_data + "/" + folder_nm + f"/distance_concat_{participant_name}." + f"/distance_concat_{participant_name}.csv"):
                        distance_df = pd.read_csv(
                            path_to_month_data + "/" + folder_nm + f"/distance_concat_{participant_name}." + f"/distance_concat_{participant_name}.csv")
                        list_of_distance_df.append(distance_df)
                elif "sleep" in folder_nm:
                    if os.path.exists(
                            path_to_month_data + "/" + folder_nm + f"/sleep_concat_{participant_name}." + f"/sleep_concat_{participant_name}.csv"):
                        sleep_df = pd.read_csv(
                            path_to_month_data + "/" + folder_nm + f"/sleep_concat_{participant_name}." + f"/sleep_concat_{participant_name}.csv")
                        list_of_sleep_df.append(sleep_df)
                elif "step" in folder_nm:
                    if os.path.exists(
                            path_to_month_data + "/" + folder_nm + f"/step_concat_{participant_name}." + f"/step_concat_{participant_name}.csv"):
                        step_df = pd.read_csv(
                            path_to_month_data + "/" + folder_nm + f"/step_concat_{participant_name}." + f"/step_concat_{participant_name}.csv")
                        list_of_step_df.append(step_df)

    # Calories
    cal_columns_takeaway = ['State', 'Calories', 'Interpreted']
    multi_month_file_concat(list_of_cal_df, path_to_fitbit_data, participant_name, "calories", "cal",
                            cal_columns_takeaway)
    # Distance
    distance_columns_concat = ['Distance']
    multi_month_file_concat(list_of_distance_df, path_to_fitbit_data, participant_name, "distance", "distance",
                            distance_columns_concat)
    # Heart
    heart_columns_concat = ['Heart Rate']
    multi_month_file_concat(list_of_heart_df, path_to_fitbit_data, participant_name, "heart", "heart",
                            heart_columns_concat)
    # Step
    step_columns_concat = ['Step']
    multi_month_file_concat(list_of_step_df, path_to_fitbit_data, participant_name, "step", "step",
                            step_columns_concat)
    # Sleep
    sleep_columns_concat = ['State', 'Interpreted']
    multi_month_file_concat(list_of_sleep_df, path_to_fitbit_data, participant_name, "sleep", "sleep",
                            sleep_columns_concat)


def multi_month_data_pp_by_term(file_path_to_term):
    """
    Creates single concat feature folders and files
    for participants whose data is grouped by month and already been
    preprocessed for each month individually for every student inside the
    "file_path_to_term" directory

    :param file_path_to_term: directory path to term
    :return: void
    """
    for student_name in os.listdir(file_path_to_term):
        if os.path.isdir(file_path_to_term + student_name):
            multi_month_data_pp(f"{file_path_to_term}{student_name}/Fitbit/", student_name)


def compile_all_term_data():
    """
    Creates a single csv file containing processed data across all terms (entire fitbit dataset)
    :return: void
    """
    term_processed_arr = []
    term_labels_arr = []

    for folder_name in os.listdir("./"):
        if os.path.isdir(
                "./" + folder_name) and "plots" not in folder_name and "temporary" not in folder_name:
            processed_data_df = pd.read_csv(
                "./" + folder_name + "/12H_term_processed_data.csv")
            term_processed_arr.append(processed_data_df)
            term_labels_df = pd.read_csv(
                "./" + folder_name + "/mean_term_student_labels.csv")
            term_labels_arr.append(term_labels_df)

    all_term_processed_data = pd.concat(term_processed_arr)
    all_term_labels_data = pd.concat(term_labels_arr)

    # Define the custom sorting order for 'Term'
    sorting_order = ["E1 term", "E2 term", "E1+E2 term", "Fall-1st cohort", "Fall-2nd cohort", "Spring-1st cohort",
                     "Spring-2nd cohort"]

    # Sort the DataFrame by 'Term' in the custom sorting order, then by 'Student ID'
    all_term_processed_data_sorted = all_term_processed_data.sort_values(by=['Term', 'Student ID', 'Time'],
                                                                         key=lambda x: x.map({term: idx for idx, term in
                                                                                              enumerate(
                                                                                                  sorting_order)}))
    all_term_processed_data_sorted.to_csv("./12H_all_term_processed_data.csv", index=False)

    all_term_labels_data_sorted = all_term_labels_data.sort_values(by=['Term', 'Student ID'], key=lambda x: x.map(
        {term: idx for idx, term in enumerate(sorting_order)}))
    all_term_labels_data_sorted.to_csv("./12H_all_mean_term_labels_processed_data.csv",
                                       index=False)


def compile_all_term_data_change_depression_label_threshold():
    """
    Creates a single csv file containing processed data across all terms (entire fitbit dataset)
    :return: void
    """
    term_processed_arr = []
    term_labels_arr = []

    for folder_name in os.listdir("./"):
        if os.path.isdir("./" + folder_name):
            if not (os.path.exists("./" + folder_name + "/term_processed_data.csv")
                    and os.path.exists(
                        "./" + folder_name + "/mean_term_student_labels.csv")):
                continue

            processed_data_df = pd.read_csv("./" + folder_name + "/term_processed_data.csv")
            term_processed_arr.append(processed_data_df)
            term_labels_df = pd.read_csv(
                "./" + folder_name + "/mean_term_student_labels.csv")
            term_labels_arr.append(term_labels_df)

            all_term_processed_data = pd.concat(term_processed_arr)
            all_term_labels_data = pd.concat(term_labels_arr)

            # Update State
            all_term_processed_data['CESD State'] = all_term_processed_data['CESD'].apply(lambda x: assign_state(x))
            all_term_labels_data['CESD State'] = all_term_labels_data['CESD (Mean) Label'].apply(
                lambda x: assign_state(x))

            # Update State Interpretation
            all_term_processed_data['CESD State Interpreted'] = all_term_processed_data['CESD State'].apply(
                lambda x: assign_interpretation(x))
            all_term_labels_data['CESD State Interpreted'] = all_term_labels_data['CESD State'].apply(
                lambda x: assign_interpretation(x))

            # Define the custom sorting order for 'Term'
            sorting_order = ["E1 Term", "E2 Term", "E1+E2 term", "Fall-1st cohort", "Fall-2nd cohort",
                             "Spring-1st cohort",
                             "Spring-2nd cohort"]

            # Sort the DataFrame by 'Term' in the custom sorting order, then by 'Student ID'
            all_term_processed_data_sorted = all_term_processed_data.sort_values(by=['Term', 'Student ID', 'Time'],
                                                                                 key=lambda x: x.map(
                                                                                     {term: idx for idx, term in
                                                                                      enumerate(
                                                                                          sorting_order)}))
            all_term_processed_data_sorted.to_csv("./all_term_processed_data.csv",
                                                  index=False)

            all_term_labels_data_sorted = all_term_labels_data.sort_values(by=['Term', 'Student ID'],
                                                                           key=lambda x: x.map(
                                                                               {term: idx for idx, term in
                                                                                enumerate(sorting_order)}))
            all_term_labels_data_sorted.to_csv("./all_mean_term_labels_processed_data.csv",
                                               index=False)


# Updates the CESD State and Interpretation of the processed sum for each term
def update_cesd_state_interp_for_term_individually():
    """
    Update CESD state and interpretation for each term individually.

    Note: This function is to be called whenever you make a change to the depression
    threshold (ex: >= 16 means 'Depressed' in our case)

    :return: void
    """
    for folder_name in os.listdir("./"):
        if os.path.isdir("./" + folder_name):
            if not (os.path.exists("./" + folder_name + "/term_processed_data.csv")
                    and os.path.exists(
                        "./" + folder_name + "/mean_term_student_labels.csv")):
                continue

            # get term processed data
            term_pc_df = pd.read_csv("./" + folder_name + "/term_processed_data.csv")
            term_label_df = pd.read_csv(
                "./" + folder_name + "/mean_term_student_labels.csv")

            # Update State
            term_pc_df['CESD State'] = term_pc_df['CESD'].apply(lambda x: assign_state(x))
            term_label_df['CESD State'] = term_label_df['CESD (Mean) Label'].apply(lambda x: assign_state(x))

            # Update State Interpretation
            term_pc_df['CESD State Interpreted'] = term_pc_df['CESD State'].apply(
                lambda x: assign_interpretation(x))
            term_label_df['CESD State Interpreted'] = term_label_df['CESD State'].apply(
                lambda x: assign_interpretation(x))

            term_pc_df.to_csv("./" + folder_name + "/term_processed_data.csv", index=False)
            term_label_df.to_csv("./" + folder_name + "/mean_term_student_labels.csv",
                                 index=False)


def create_specific_term_labels_csv(file_path_to_raw_data, type_column_name, type_metric, value):
    """
    Create specific term labels csv based on the provided data.

    :param file_path_to_raw_data: The path to the raw data files.
    :param type_column_name: The name of the column representing the label type. (ex: 'Max', 'Mean', 'Last')
    :param type_metric: The label type. (ex: 'max', 'mean', 'last')
    :param value: The value representing the label creation method.

    :return: void
    """
    for folder_name in os.listdir(file_path_to_raw_data):
        if os.path.isdir(file_path_to_raw_data + folder_name) and (
                "term" in folder_name or "cohort" in folder_name) and "E1+E2 term" in folder_name:
            term_data_df = pd.read_csv(file_path_to_raw_data + folder_name + "/term_processed_data.csv")
            filtered_term_data_df = term_data_df[['Student ID', 'CESD']].dropna()
            grouped_data = filtered_term_data_df.groupby('Student ID')['CESD'].unique()
            if value == 1:
                # create label by max
                mean_grouped_data = grouped_data.apply(lambda x: x.max())
            elif value == 0:
                # create label by mean
                mean_grouped_data = grouped_data.apply(lambda x: x.mean())
            else:
                # create label by more recent survey score
                mean_grouped_data = grouped_data.apply(lambda x: x[-1])

            final_label_df = pd.DataFrame({f'CESD ({type_column_name}) Label': mean_grouped_data})
            print(mean_grouped_data)
            final_label_df['CESD State'] = final_label_df[f'CESD ({type_column_name}) Label'].apply(
                lambda x: assign_state(x))
            final_label_df['CESD State Interpreted'] = final_label_df['CESD State'].apply(
                lambda x: assign_interpretation(x))
            final_label_df['Term'] = folder_name
            final_label_df_up = final_label_df[
                ['Term', f'CESD ({type_column_name}) Label', 'CESD State', 'CESD State Interpreted']]
            final_label_df_up.to_csv(
                f"./{folder_name}/{type_metric}_term_student_labels.csv")


def create_new_term_processed_data_csv(file_path_to_raw_data, type_metric, type_column_name):
    """
    Create new processed term data based on the provided raw data and labels.

    :param file_path_to_raw_data: The path to the raw data files.
    :param type_column_name:  The name of the file containing the labels.
    :param type_metric: The type of data being processed. (ex: 'max', 'mean', 'last')

    :return: void
    """
    for folder_name in os.listdir(file_path_to_raw_data):
        if os.path.isdir(file_path_to_raw_data + folder_name) and ("term" in folder_name or "cohort" in folder_name):
            term_processed_data_df = pd.read_csv(file_path_to_raw_data + folder_name + f"/term_processed_data.csv")
            type_term_labels_df = pd.read_csv(
                file_path_to_raw_data + folder_name + f"/{type_metric}_term_student_labels.csv")

            for index, row in type_term_labels_df.iterrows():
                current_student_id = row['Student ID']
                current_student_cesd_score = row[f'CESD ({type_column_name}) Label']
                term_processed_data_df.loc[
                    term_processed_data_df['Student ID'] == current_student_id, 'CESD'] = current_student_cesd_score

            # Update State
            term_processed_data_df['CESD State'] = term_processed_data_df['CESD'].apply(lambda x: assign_state(x))

            # Update State Interpretation
            term_processed_data_df['CESD State Interpreted'] = term_processed_data_df['CESD State'].apply(
                lambda x: assign_interpretation(x))

            term_processed_data_df.rename(columns={'CESD': f'CESD ({type_column_name})'}, inplace=True)
            term_processed_data_df.to_csv(
                file_path_to_raw_data + folder_name + f"/{type_metric}_term_processed_data.csv", index=False)


def extrapolate_data_by_term_final(file_path_to_term, sample_size, start_date, end_date):
    """
    Creates a csv file in the directory of each student in "file_path_to_term" containing all of their
    sampled time series data into a single data frame, along with their depression survey scores

    Note: This function is to be called after every student's data in the
    "file_path_to_term" directory has been preprocessed. For more information
    look at the data preprocessing functions in: data_preprocess_functions.py

    :param end_date: end date of term
    :param start_date: start date of term
    :param file_path_to_term: directory path to term data
    :param sample_size: the type of sampling: (ex: hour, day, week, month)
    :return: A csv file in the directory of each student in "file_path_to_term"
    """
    # Iterate through each student folder in the specified term directory
    for folder_name in os.listdir(file_path_to_term):
        if "invalid" not in folder_name and "plots" not in folder_name and "temporary" not in folder_name:
            folder_full_path = os.path.join(file_path_to_term, folder_name)
            if os.path.isdir(folder_full_path):

                path_to_survey_data = folder_full_path + "/Survey/"
                survey_excel_path, survey_excel_file_name = extract_survey_excel_path(path_to_survey_data)

                path_to_fitbit_data = folder_full_path + "/Fitbit/"

                final_df_columns_arr = ['Student ID']

                #if os.path.exists(
                for filename in os.listdir(path_to_fitbit_data):
                    if filename.startswith("heart"):
                        #path_to_fitbit_data + "heart_" + folder_name + "/heart_concat_" + folder_name + "." + "/heart_concat_" + folder_name + ".csv"):
                        #path_to_fitbit_data + "heart20200".csv"):
                        file_path = os.path.join(path_to_fitbit_data, filename)
                        df_heart_concat = pd.read_csv(
                        #path_to_fitbit_data + "heart_" + folder_name + "/heart_concat_" + folder_name + "." + "/heart_concat_" + folder_name + ".csv")
                            file_path)
                        heart_data_exists = True
                    else:
                        print(f"Heart Rate data for {folder_name} does not exist")
                        df_heart_concat = pd.DataFrame(columns=['Heart Rate (Mean)'])
                        heart_data_exists = False

                    final_df_columns_arr.append('Heart Rate (Mean)')

                if os.path.exists(
                        path_to_fitbit_data + "calories_" + folder_name + "/cal_concat_" + folder_name + "." + "/cal_concat_" + folder_name + ".csv"):
                    df_calories_concat = pd.read_csv(
                        path_to_fitbit_data + "calories_" + folder_name + "/cal_concat_" + folder_name + "." + "/cal_concat_" + folder_name + ".csv")
                    calories_exists = True
                else:
                    print(f"Calories data for {folder_name} does not exist")
                    df_calories_concat = pd.DataFrame(columns=["Calories (Sum)", "Cal_State", "Cal_State_Interpreted"])
                    calories_exists = False

                final_df_columns_arr.append('Calories (Sum)')
                final_df_columns_arr.append('Cal_State')
                final_df_columns_arr.append('Cal_State_Interpreted')

                if os.path.exists(
                        path_to_fitbit_data + "distance_" + folder_name + "/distance_concat_" + folder_name + "." + "/distance_concat_" + folder_name + ".csv"):
                    df_distance_concat = pd.read_csv(
                        path_to_fitbit_data + "distance_" + folder_name + "/distance_concat_" + folder_name + "." + "/distance_concat_" + folder_name + ".csv")
                    distance_data_exists = True
                else:
                    print(f"Distance data for {folder_name} does not exist")
                    df_distance_concat = pd.DataFrame(columns=["Distance (Sum)"])
                    distance_data_exists = False

                final_df_columns_arr.append('Distance (Sum)')

                if os.path.exists(
                        path_to_fitbit_data + "step_" + folder_name + "/step_concat_" + folder_name + "." + "/step_concat_" + folder_name + ".csv"):
                    df_step_concat = pd.read_csv(
                        path_to_fitbit_data + "step_" + folder_name + "/step_concat_" + folder_name + "." + "/step_concat_" + folder_name + ".csv")
                    step_data_exists = True
                else:
                    print(f"Step data for {folder_name} does not exist")
                    df_step_concat = pd.DataFrame(columns=["Step (Sum)"])
                    step_data_exists = False

                final_df_columns_arr.append('Step (Sum)')

                if os.path.exists(
                        path_to_fitbit_data + "sleep_" + folder_name + "/sleep_concat_" + folder_name + "." + "/sleep_concat_" + folder_name + ".csv"):
                    df_sleep_concat = pd.read_csv(
                        path_to_fitbit_data + "sleep_" + folder_name + "/sleep_concat_" + folder_name + "." + "/sleep_concat_" + folder_name + ".csv")
                    sleep_data_exists = True
                else:
                    print(f"Sleep data for {folder_name} does not exist")
                    df_sleep_concat = pd.DataFrame(columns=["Sleep_State", "Sleep_State_Interpreted"])
                    sleep_data_exists = False

                final_df_columns_arr.append('Sleep_State')
                final_df_columns_arr.append('Sleep_State_Interpreted')

                # Skip student if they have no data
                if not (
                        sleep_data_exists or step_data_exists or distance_data_exists or heart_data_exists or calories_exists):
                    continue

                final_df_columns_arr.append('CESD')

                calories_sampled = calorie_set_up(df_calories_concat, sample_size)
                heart_rate_sampled = heart_set_up(df_heart_concat, sample_size)
                distance_sampled = distance_set_up(df_distance_concat, sample_size)
                sleep_sampled = sleep_set_up(df_sleep_concat, sample_size)
                step_sampled = step_set_up(df_step_concat, sample_size)
                survey_data_sampled = survey_data_set_up(survey_excel_path)

                # Merge the data frames
                df_cal_heart_merge = pd.merge(calories_sampled, heart_rate_sampled, left_index=True, right_index=True,
                                              how='outer')
                df_distance_sleep_merge = pd.merge(distance_sampled, sleep_sampled, left_index=True, right_index=True,
                                                   how='outer')
                df_step_survey_merge = pd.merge(step_sampled, survey_data_sampled, left_index=True, right_index=True,
                                                how='outer')

                df_cal_heart_distance_sleep_merge = pd.merge(df_cal_heart_merge, df_distance_sleep_merge,
                                                             left_index=True,
                                                             right_index=True, how='outer')
                df_all_features_plus_survey = pd.merge(df_cal_heart_distance_sleep_merge, df_step_survey_merge,
                                                       left_index=True, right_index=True, how='outer')

                # Create masking columns

                # CESD Score Mask
                df_all_features_plus_survey['CESD Score Mask'] = 0
                final_df_columns_arr.append('CESD Score Mask')
                df_all_features_plus_survey.loc[df_all_features_plus_survey['CESD'].isnull(), 'CESD Score Mask'] = 1

                # Heart Rate Mask
                df_all_features_plus_survey['Heart Rate Mask'] = 0
                final_df_columns_arr.append('Heart Rate Mask')
                df_all_features_plus_survey.loc[
                    df_all_features_plus_survey['Heart Rate (Mean)'].isnull(), 'Heart Rate Mask'] = 1

                # Calories Burned Mask
                df_all_features_plus_survey['Calories Burned Mask'] = 0
                final_df_columns_arr.append('Calories Burned Mask')
                df_all_features_plus_survey.loc[
                    df_all_features_plus_survey['Calories (Sum)'].isnull(), 'Calories Burned Mask'] = 1

                # Distance Traveled Mask
                df_all_features_plus_survey['Distance Traveled Mask'] = 0
                final_df_columns_arr.append('Distance Traveled Mask')
                df_all_features_plus_survey.loc[
                    df_all_features_plus_survey['Distance (Sum)'].isnull(), 'Distance Traveled Mask'] = 1

                # Steps Taken Mask
                df_all_features_plus_survey['Steps Taken Mask'] = 0
                final_df_columns_arr.append('Steps Taken Mask')
                df_all_features_plus_survey.loc[
                    df_all_features_plus_survey['Step (Sum)'].isnull(), 'Steps Taken Mask'] = 1

                # Set Depression Scores
                # current = df_all_features_plus_survey['CESD'].iloc[0]
                # for index, row in df_all_features_plus_survey.iterrows():
                #     if not pd.isna(row['CESD']):
                #         current = row['CESD']
                #     else:
                #         df_all_features_plus_survey.at[index, 'CESD'] = current

                # Grabs student id from survey file name
                if "Fall" in file_path_to_term or "Spring" in file_path_to_term:
                    student_id_value = survey_excel_file_name[1:len(survey_excel_file_name) - 5]
                else:
                    student_id_value = survey_excel_file_name[6:len(survey_excel_file_name) - 5]

                df_all_features_plus_survey['Student ID'] = student_id_value

                df_all_features_plus_survey_final = df_all_features_plus_survey[final_df_columns_arr]

                # Merge df_all_features_plus_survey_final with desired date range

                desired_time_interval_df = pd.DataFrame(
                    {'Time': pd.date_range(start=start_date, end=end_date, freq=sample_size)})

                merged_df = pd.merge(desired_time_interval_df, df_all_features_plus_survey_final, on='Time', how='left')

                merged_df['Student ID'] = student_id_value

                # Update the 'Score Mask' column where 'Score' is missing
                merged_df.loc[merged_df['CESD'].isnull(), 'CESD Score Mask'] = 1
                merged_df.loc[merged_df['Heart Rate (Mean)'].isnull(), 'Heart Rate Mask'] = 1
                merged_df.loc[merged_df['Calories (Sum)'].isnull(), 'Calories Burned Mask'] = 1
                merged_df.loc[merged_df['Distance (Sum)'].isnull(), 'Distance Traveled Mask'] = 1
                merged_df.loc[merged_df['Step (Sum)'].isnull(), 'Steps Taken Mask'] = 1

                current = merged_df['CESD'].iloc[0]
                for index, row in merged_df.iterrows():
                    if not pd.isna(row['CESD']):
                        current = row['CESD']
                    else:
                        merged_df.at[index, 'CESD'] = current

                mean_hr_value = merged_df['Heart Rate (Mean)'].mean()
                merged_df['Heart Rate (Mean)'].fillna(mean_hr_value, inplace=True)

                calories_burned_value = merged_df['Calories (Sum)'].mean()
                merged_df['Calories (Sum)'].fillna(calories_burned_value, inplace=True)

                distance_traveled_value = merged_df['Distance (Sum)'].mean()
                merged_df['Distance (Sum)'].fillna(distance_traveled_value, inplace=True)

                steps_taken_value = merged_df['Step (Sum)'].mean()
                merged_df['Step (Sum)'].fillna(steps_taken_value, inplace=True)

                unique_values = df_all_features_plus_survey_final['CESD'].unique()
                mean_cesd_val = np.nanmean(unique_values)
                merged_df['CESD'].fillna(mean_cesd_val, inplace=True)

                # Drop Time Duplicates (If Any)
                merged_df_final = merged_df.drop_duplicates(subset='Time')
                merged_df_final.to_csv(folder_full_path + "/12H_processed_" + folder_name + ".csv", index=False)


def convert_term_data_proper_form(term, feature_column, feature_for_file_naming, feature_feat_col_name, filename, timeofdaycap, timeofdaylow):
    """

    :param term: is the term containing the data (ex: E1 term, E2 term...)
    :param feature_column: the name of the feature as spelled in the "term_processed_data" file (ex: Heart Rate (Mean))
    :param feature_for_file_naming: feature name for file naming
    :param feature_feat_col_name: name you want to give as value for the 'Feature' column of reshaped file
    :return: CSV file: 85_{feature_for_file_naming}_term_reshaped_data.csv containing reshaped data for specific term
    """

    all_data_term = pd.read_csv(f"./{term}/{filename}.csv")
    all_data_term['Time Step'] = all_data_term.groupby('Student ID').cumcount() + 1
    all_data_term_filtered = all_data_term[
        ['Time Step', 'Time', 'Student ID', 'Term', feature_column, 'CESD']]

    # Get Unique CESD Scores For Each Student
    unique_stores_series = all_data_term_filtered.groupby('Student ID')['CESD'].unique()
    mean_grouped_data = unique_stores_series.apply(lambda x: x.mean())

    unique_term_series = all_data_term_filtered.groupby('Student ID')['Term'].unique()
    unique_term_series_df = unique_term_series.explode().reset_index()

    all_data_e1_term_filtered_shaped = all_data_term_filtered.pivot(index='Student ID', columns=['Time Step'],
                                                                    values=feature_column)
    all_data_e1_term_filtered_shaped['CESD (Mean) Label'] = mean_grouped_data

    all_data_e1_term_filtered_shaped = all_data_e1_term_filtered_shaped.reset_index()

    merged_df = pd.merge(all_data_e1_term_filtered_shaped, unique_term_series_df, on='Student ID', how='inner')

    merged_df['Feature'] = feature_feat_col_name
    merged_df['CESD (Mean) Label'] = mean_grouped_data.values
    merged_df['CESD State'] = merged_df['CESD (Mean) Label'].apply(lambda x: assign_state(x))
    merged_df['CESD State Interpreted'] = merged_df['CESD State'].apply(lambda x: assign_interpretation(x))

    # Includes all days in shaped format
    # new_column_order = ['Student ID'] + ['Term'] + ['Feature'] + list(
    #     merged_df.columns[1:len(merged_df.columns) - 5]) + ['CESD (Mean) Label'] + ['CESD State'] + [
    #                        'CESD State Interpreted']

    # Includes only 85 days
    new_column_order = ['Student ID'] + ['Term'] + ['Feature'] + list(
        merged_df.columns[1:86]) + ['CESD (Mean) Label'] + ['CESD State'] + [
                           'CESD State Interpreted']

    merged_df_filterd = merged_df[new_column_order]

    if not os.path.exists(f"./{term}/12H Data/{timeofdaycap}/"):
        os.makedirs(f"./{term}/12H Data/{timeofdaycap}/")

    merged_df_filterd.to_csv(
        f"./{term}/12H Data/{timeofdaycap}/85_12H_{timeofdaylow}_{feature_for_file_naming}_term_reshaped_data.csv",
        index=False)


def format_all_term_processed_data():
    all_term_processed_df_filtered_col = pd.read_csv("./12H_day_data_all_term.csv")

    all_term_processed_df_filtered_col_sampled = all_term_processed_df_filtered_col.groupby('Student ID').apply(
        lambda x: x.head(min(len(x), 85))).reset_index(drop=True)

    sorted_df = all_term_processed_df_filtered_col_sampled.sort_values(by=['Student ID', 'Time']).reset_index(drop=True)

    # Add 'Time Step' column
    sorted_df['Time Step'] = sorted_df.groupby('Student ID').cumcount() + 1

    sorted_df = sorted_df[
        ['Time Step'] + [col for col in sorted_df.columns if col != 'Time Step']]

    sorted_df.to_csv("./formatted_12H_day_data_all_term.csv", index=False)


def compile_proper_form_data():
    term_reshape_arr = []
    for folder_name in os.listdir("./"):
        if os.path.exists("./" + folder_name + "/85_term_reshaped_data.csv"):
            data = pd.read_csv("./" + folder_name + "/85_term_reshaped_data.csv")
            term_reshape_arr.append(data)

    file_concat = pd.concat(term_reshape_arr).reset_index(drop=True)

    # Define the custom sorting order for 'Term'
    sorting_order = ["E1 term", "E2 term", "E1+E2 term", "Fall-1st cohort", "Fall-2nd cohort", "Spring-1st cohort",
                     "Spring-2nd cohort"]

    # Sort the DataFrame by 'Term' in the custom sorting order, then by 'Student ID'
    file_concat_sorted = file_concat.sort_values(by=['Term', 'Student ID'],
                                                 key=lambda x: x.map({term: idx for idx, term in
                                                                      enumerate(
                                                                          sorting_order)}))

    file_concat_sorted.to_csv("./format_shaped_all_term_data.csv", index=False)


def extract_sleep_data(term):
    sleep_all_term_data = pd.read_csv("./sleep_all_term_processed_data.csv")
    e1_term_processed_data = pd.read_csv(f"./{term}/term_processed_data.csv")

    sleep_all_term_data_e1_term = sleep_all_term_data[sleep_all_term_data['Term'] == term]

    # Assuming your dataframe is named 'sleep_all_term_data_e1_term_filtered'
    sleep_all_term_data_e1_term['Hours_Asleep Mask'] = sleep_all_term_data_e1_term['Hours_Asleep'].apply(
        lambda x: 1 if pd.isna(x) else 0)
    sleep_all_term_data_e1_term['Minutes_Asleep Mask'] = sleep_all_term_data_e1_term['Minutes_Asleep'].apply(
        lambda x: 1 if pd.isna(x) else 0)

    sleep_all_term_data_e1_term_filtered = sleep_all_term_data_e1_term[
        ['Time', 'Student ID', 'Term', 'Minutes_Asleep', 'Minutes_Asleep Mask', 'Hours_Asleep', 'Hours_Asleep Mask']]

    # Impute term hours asleep
    sleep_all_term_data_e1_term_filtered['Hours_Asleep'] = sleep_all_term_data_e1_term_filtered.groupby('Student ID')[
        'Hours_Asleep'].transform(lambda x: x.fillna(x.mean()))
    sleep_all_term_data_e1_term_filtered['Minutes_Asleep'] = sleep_all_term_data_e1_term_filtered.groupby('Student ID')[
        'Minutes_Asleep'].transform(lambda x: x.fillna(x.mean()))

    sleep_all_term_data_e1_term_filtered.to_csv(f"./{term}/sleep_term_data.csv",
                                                index=False)


def merge_sleep_to_term_processed_data(term):
    e1_term_processed_data = pd.read_csv(f"./{term}/12H_term_processed_data.csv")
    e1_term_sleep_data = pd.read_csv(f"./{term}/sleep_term_data.csv")
    e1_term_sleep_data_filtered = e1_term_sleep_data[
        ['Time', 'Student ID', 'Minutes_Asleep', 'Minutes_Asleep Mask', 'Hours_Asleep', 'Hours_Asleep Mask']]

    merged_df = pd.merge(e1_term_processed_data, e1_term_sleep_data_filtered, on=['Time', 'Student ID'])
    merged_df_filtered = merged_df[
        ['Time', 'Student ID', 'Term', 'Heart Rate (Mean)', 'Heart Rate Mask', 'Calories (Sum)',
         'Calories Burned Mask',
         'Cal_State', 'Cal_State_Interpreted', 'Distance (Sum)', 'Distance Traveled Mask', 'Step (Sum)',
         'Steps Taken Mask', 'Minutes_Asleep', 'Minutes_Asleep Mask', 'Hours_Asleep', 'Hours_Asleep Mask',
         'Sleep_State', 'Sleep_State_Interpreted', 'CESD', 'CESD Score Mask', 'CESD State',
         'CESD State Interpreted']]
    merged_df_filtered.to_csv(f"./{term}/12H_term_processed_data.csv", index=False)


def concat_all_all_feature_term_reshaped_data(term):
    reshaped_data_arr = []

    for file in os.listdir(f"./{term}/"):
        if "85" in file:
            df = pd.read_csv(f"./{term}/" + file)
            reshaped_data_arr.append(df)

    concat_reshaped_df = pd.concat(reshaped_data_arr)

    # Create the feature sorting order list
    feature_sorting_order = ["Heart Rate", "Calories Burned", "Distance Traveled", "Steps Taken", "Hours Slept"]

    # Create a Categorical data type with the desired sorting order
    feature_cat = pd.CategoricalDtype(categories=feature_sorting_order, ordered=True)

    # Assign the "Feature" column of your DataFrame to the Categorical data type
    concat_reshaped_df['Feature'] = concat_reshaped_df['Feature'].astype(feature_cat)

    # Sort the DataFrame by 'Student ID' and 'Feature'
    concat_reshaped_df_sorted = concat_reshaped_df.sort_values(by=['Student ID', 'Feature'])

    concat_reshaped_df_sorted.to_csv(f"./{term}/all_feature_term_reshaped_data.csv",
                                     index=False)


def convert_full_dataset_data_proper_form(term, filename, timeofdaycap, timeofdaylow):
    convert_term_data_proper_form(f"{term}", "Heart Rate (Mean)", "heart", "Heart Rate", filename, timeofdaycap, timeofdaylow)
    convert_term_data_proper_form(f"{term}", "Calories (Sum)", "cal", "Calories Burned", filename, timeofdaycap, timeofdaylow)
    convert_term_data_proper_form(f"{term}", "Distance (Sum)", "distance", "Distance Traveled", filename, timeofdaycap, timeofdaylow)
    convert_term_data_proper_form(f"{term}", "Step (Sum)", "step", "Steps Taken", filename, timeofdaycap, timeofdaylow)
    # convert_term_data_proper_form(f"{term}", "Hours_Asleep", "sleep_hours", "Hours Slept")


def setup_whole_term(term, start_date, end_date):
    extrapolate_data_by_term_final(f"./{term}/", 'D', start_date, end_date)
    concat_student_processed_data(f"./{term}/")
    add_cesd_term_labels_pd(f"{term}")
    merge_sleep_to_term_processed_data(f"{term}")
    convert_full_dataset_data_proper_form(f"{term}")
    concat_all_all_feature_term_reshaped_data(f"{term}")


def create_all_feature_shaped_data():
    all_feature_shaped_data_arr = []

    for folder_name in os.listdir("./"):
        if os.path.isdir(
                "./" + folder_name) and "temporary" not in folder_name and "plots" not in folder_name:
            all_feature_shaped_data_df = pd.read_csv(
                "./" + folder_name + "/all_feature_term_reshaped_data.csv")
            all_feature_shaped_data_arr.append(all_feature_shaped_data_df)

    concat_all_feature_shaped = pd.concat(all_feature_shaped_data_arr)

    # Create the feature sorting order list
    feature_sorting_order = ["Heart Rate", "Calories Burned", "Distance Traveled", "Steps Taken", "Hours Slept"]

    # Create a Categorical data type with the desired sorting order
    feature_cat = pd.CategoricalDtype(categories=feature_sorting_order, ordered=True)

    # Assign the "Feature" column of your DataFrame to the Categorical data type
    concat_all_feature_shaped['Feature'] = concat_all_feature_shaped['Feature'].astype(feature_cat)

    # Define the custom sorting order for 'Term'
    sorting_order = ["E1 term", "E2 term", "E1+E2 term", "Fall-1st cohort", "Fall-2nd cohort", "Spring-1st cohort",
                     "Spring-2nd cohort"]

    # Sort the DataFrame by 'Term' in the custom sorting order, then by 'Student ID'
    concat_reshaped_df_sorted = concat_all_feature_shaped.sort_values(by=['Student ID', 'Feature', 'Term'],
                                                                      key=lambda x: x.map({term: idx for idx, term in
                                                                                           enumerate(
                                                                                               sorting_order)}))

    concat_reshaped_df_sorted.to_csv("./format_shaped_all_term_data.csv", index=False)


def function_12h(term, start_date, end_date):
    extrapolate_data_by_term_final(f"./{term}/", '12H', start_date, end_date)
    concat_student_processed_data(f"./{term}/")
    add_cesd_term_labels_pd(term)
    merge_sleep_to_term_processed_data(f"{term}")


extrapolate_data_by_term_final("./E1 term/", '12H', '2020-06-15', '2020-09-15')
concat_student_processed_data("./E1 term/")
add_cesd_term_labels_pd("E1 term")


# function_12h("E2 term", '2020-06-15', '2020-10-26')
# function_12h("E1+E2 term", '2020-06-15', '2020-10-16')
# function_12h("Fall-1st cohort", '2020-09-06', '2020-12-18')
# function_12h("Fall-2nd cohort", '2020-09-07', '2020-12-23')
# function_12h("Spring-1st cohort", '2021-02-15', '2021-05-16')
# function_12h("Spring-2nd cohort", '2021-04-05', '2021-06-28')

compile_all_term_data()

convert_full_dataset_data_proper_form("E1 term")
# convert_full_dataset_data_proper_form("E2 term", "day_data_e2term", "Day", "day")
# convert_full_dataset_data_proper_form("E2 term", "night_data_e2term", "Night", "night")
#
# convert_full_dataset_data_proper_form("E1+E2 term", "day_data_e1+e2term", "Day", "day")
# convert_full_dataset_data_proper_form("E1+E2 term", "night_data_e1+e2term", "Night", "night")
#
# convert_full_dataset_data_proper_form("Fall-1st cohort", "day_data_fall1stcohort", "Day", "day")
# convert_full_dataset_data_proper_form("Fall-1st cohort", "night_data_fall1stcohort", "Night", "night")
#
# convert_full_dataset_data_proper_form("Fall-2nd cohort", "day_data_fall2ndcohort", "Day", "day")
# convert_full_dataset_data_proper_form("Fall-2nd cohort", "night_data_fall2ndcohort", "Night", "night")
#
# convert_full_dataset_data_proper_form("Spring-1st cohort", "day_data_spring1stcohort", "Day", "day")
# convert_full_dataset_data_proper_form("Spring-1st cohort", "night_data_spring1stcohort", "Night", "night")
#
# convert_full_dataset_data_proper_form("Spring-2nd cohort", "day_data_spring2ndcohort", "Day", "day")
# convert_full_dataset_data_proper_form("Spring-2nd cohort", "night_data_spring2ndcohort", "Night", "night")


# convert_full_dataset_data_proper_form("E1+E2 term")
# convert_full_dataset_data_proper_form("Fall-1st cohort")
# convert_full_dataset_data_proper_form("Fall-2nd cohort")
# convert_full_dataset_data_proper_form("Spring-1st cohort")
# convert_full_dataset_data_proper_form("Spring-2nd cohort")


create_new_term_processed_data_csv("./", "mean", "Mean")

format_all_term_processed_data()

create_all_feature_shaped_data

def concat_all_feature_term_reshaped_data_12H(term, timeofdaycap, timeofdaylow):
    reshaped_data_arr = []

    for file in os.listdir(f"./{term}/12H Data/{timeofdaycap}/"):
        if "85" in file:
            df = pd.read_csv(f"./{term}/12H Data/{timeofdaycap}/" + file)
            reshaped_data_arr.append(df)

    concat_reshaped_df = pd.concat(reshaped_data_arr)

    # Create the feature sorting order list
    feature_sorting_order = ["Heart Rate", "Calories Burned", "Distance Traveled", "Steps Taken"]

    # Create a Categorical data type with the desired sorting order
    feature_cat = pd.CategoricalDtype(categories=feature_sorting_order, ordered=True)

    # Assign the "Feature" column of your DataFrame to the Categorical data type
    concat_reshaped_df['Feature'] = concat_reshaped_df['Feature'].astype(feature_cat)

    # Sort the DataFrame by 'Student ID' and 'Feature'
    concat_reshaped_df_sorted = concat_reshaped_df.sort_values(by=['Student ID', 'Feature'])

    concat_reshaped_df_sorted.to_csv(f"./{term}/12H Data/{timeofdaycap}/all_{timeofdaylow}_feature_term_reshaped_data.csv",
                                     index=False)


def create_all_feature_shaped_data_12H(timeofdaycap, timeofdaylow):
    all_feature_shaped_data_arr = []

    for folder_name in os.listdir("./"):
        if os.path.isdir(
                "./" + folder_name) and "temporary" not in folder_name and "plots" not in folder_name:
            all_feature_shaped_data_df = pd.read_csv(
                "./" + folder_name + f"/12H Data/{timeofdaycap}/all_{timeofdaylow}_feature_term_reshaped_data.csv")
            all_feature_shaped_data_arr.append(all_feature_shaped_data_df)

    concat_all_feature_shaped = pd.concat(all_feature_shaped_data_arr)

    # Create the feature sorting order list
    feature_sorting_order = ["Heart Rate", "Calories Burned", "Distance Traveled", "Steps Taken"]

    # Create a Categorical data type with the desired sorting order
    feature_cat = pd.CategoricalDtype(categories=feature_sorting_order, ordered=True)

    # Assign the "Feature" column of your DataFrame to the Categorical data type
    concat_all_feature_shaped['Feature'] = concat_all_feature_shaped['Feature'].astype(feature_cat)

    # Define the custom sorting order for 'Term'
    sorting_order = ["E1 term", "E2 term", "E1+E2 term", "Fall-1st cohort", "Fall-2nd cohort", "Spring-1st cohort",
                     "Spring-2nd cohort"]

    # Sort the DataFrame by 'Term' in the custom sorting order, then by 'Student ID'
    concat_reshaped_df_sorted = concat_all_feature_shaped.sort_values(by=['Student ID', 'Feature', 'Term'],
                                                                      key=lambda x: x.map({term: idx for idx, term in
                                                                                           enumerate(
                                                                                               sorting_order)}))

    concat_reshaped_df_sorted.to_csv(f"./format_{timeofdaylow}_shaped_all_term_data.csv", index=False)


concat_all_feature_term_reshaped_data_12H("E1 term", "Day", "day")
concat_all_feature_term_reshaped_data_12H("E1 term", "Night", "night")

# concat_all_feature_term_reshaped_data_12H("E2 term", "Day", "day")
# concat_all_feature_term_reshaped_data_12H("E2 term", "Night", "night")
#
# concat_all_feature_term_reshaped_data_12H("E1+E2 term", "Day", "day")
# concat_all_feature_term_reshaped_data_12H("E1+E2 term", "Night", "night")
#
# concat_all_feature_term_reshaped_data_12H("Fall-1st cohort", "Day", "day")
# concat_all_feature_term_reshaped_data_12H("Fall-1st cohort", "Night", "night")
#
# concat_all_feature_term_reshaped_data_12H("Fall-2nd cohort", "Day", "day")
# concat_all_feature_term_reshaped_data_12H("Fall-2nd cohort", "Night", "night")
#
# concat_all_feature_term_reshaped_data_12H("Spring-1st cohort", "Day", "day")
# concat_all_feature_term_reshaped_data_12H("Spring-1st cohort", "Night", "night")
#
# concat_all_feature_term_reshaped_data_12H("Spring-2nd cohort", "Day", "day")
# concat_all_feature_term_reshaped_data_12H("Spring-2nd cohort", "Night", "night")

# create_all_feature_shaped_data_12H("Day", "day")
# create_all_feature_shaped_data_12H("Night", "night")

# print("run successful")

