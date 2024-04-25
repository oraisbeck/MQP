import os
import pandas as pd
from datetime import datetime

import survey
from survey import student


def get_date(filename):
    """
               Gets date from a file name

               :param filename: filename to extract date from
               :return: string of date
    """
    length = len(filename)
    year_frm_csv = int(filename[length - 12:length - 8])
    month_frm_csv = int(filename[length - 8:length - 6])
    day_frm_csv = int(filename[length - 6:length - 4])
    date_obj = datetime(year_frm_csv, month_frm_csv, day_frm_csv)
    formatted_date = date_obj.strftime('%Y-%m-%d')
    return formatted_date


def student(folder_path, feature, l, r, combined_data):
    """
               Runs all to get one term concatenated information on one feature

               :param folder_path: path of the term folder
               :param feature: feature(step, calories, etc.) of which data is being processed
               :param l left side of the column containing the data
               :param r right side of column containing the data
               :param combined_data data frame to put information in
               :return: dataframe containing concatenation of given feature for given student
    """
    # Iterate through all files in the folder
    for student_dir in os.listdir(folder_path):
        ID = student_dir
        student_dir_path = os.path.join(folder_path, student_dir)
        if os.path.isdir(student_dir_path):
            # Initialize an empty DataFrame for the current student
            student_data = {'Date': [], 'Sums': []}
            # Iterate through all CSV files in the student directory
            fitbit_path = os.path.join(student_dir_path, "Fitbit")
            for filename in os.listdir(fitbit_path):
                if filename.startswith(feature) and filename.endswith(".csv"):
                    file_path = os.path.join(fitbit_path, filename)
                    # Read each CSV file into a DataFrame
                    df = pd.read_csv(file_path)
                    formatted_date = get_date(filename)
                    student_data['Date'].append(formatted_date)
                    # Add another column 'Sums' with the sum of specific columns from df
                    if feature == 'sleep':
                        column_sum = df.iloc[:, l:r].eq(1).sum().sum()
                        student_data['Sums'].append(column_sum)
                    elif feature == 'heart':
                        student_data['Sums'].append(df.iloc[:, l:r].mean().values[0])
                    else:
                        student_data['Sums'].append(df.iloc[:, l:r].sum().values[0])


            # Create a DataFrame for the current student
            student_df = pd.DataFrame(student_data)
            student_df = student_df.rename(columns={'Sums': feature + "_" + ID})
            # Merge the student DataFrame with the combined data on the 'Date' column
            if combined_data.empty:
                combined_data = student_df
            else:
                combined_data = pd.merge(combined_data, student_df, on=['Date'], how='outer',
                                         suffixes=('', f'_{student_dir}'))
    return combined_data


def combine_student_data(folder_path, output_file_path, feature, l, r):
    """
               Runs all the main code for getting all the information from the term of one feature and adding it to the term CSV

               :param folder_path: path of the term folder
               :param output_file_path: path to export the csv to
               :param feature: feature(step, calories, etc.) of which data is being processed
               :param feature: feature(step, calories, etc.) of which data is being processed
               :param l left side of the column containing the data
               :param r right side of column containing the data
               :return: void
    """
    try:
        # Check if the output directory exists, and create it if not
        output_directory = os.path.dirname(output_file_path)
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        if os.path.exists(output_file_path):
            combined_data = pd.read_csv(output_file_path)
        else:
            combined_data = pd.DataFrame()

        combined_data = student(folder_path, feature, l, r, combined_data)

        # Sort the 'Date' column in ascending order
        combined_data['Date'] = pd.to_datetime(combined_data['Date'], format='%Y-%m-%d')
        combined_data = combined_data.sort_values(by='Date')

        # Write the combined data to a CSV file
        combined_data.to_csv(output_file_path, index=False)

    except Exception as e:
        print(f"Error combining student data: {e}")


def reformat(output_file_path):
    """
           Reformats the CSV file of all the data to have the all 5 files of the student next to each other.
           Adds in two rows, the first containing just the ID of the student and the second containing just the feature of that column

           :param output_file_path: path to export the csv to
           :return: void
    """
    df = pd.read_csv(output_file_path)

    # Group columns by their suffixes
    df["Date"] = range(1, len(df) + 1)
    grouped_columns = {}
    for col in df.columns:
        if col == "Date":
            grouped_columns["Date"] = [col]
        else:
            prefix, suffix = col.rsplit('_', 1)
            if suffix not in grouped_columns:
                grouped_columns[suffix] = []
            grouped_columns[suffix].append(col)

    # Combine the grouped columns
    new_order = [col for suffix in grouped_columns.values() for col in suffix]

    # Create a new DataFrame with the rearranged columns
    df_rearranged = df[new_order]

    # Save the new DataFrame to a new CSV file
    df_rearranged.to_csv(output_file_path, index=False)


    print(output_file_path + " created")


def run_all(folder_path, output_file_path):
    """
               Runs all methods to export csv files for each cohort

               :param folder_path: path of the cohort
               :param output_file_path: path to export the csv to
               :return: void
    """
    combine_student_data(folder_path, output_file_path, "ID", 1, 2)
    combine_student_data(folder_path, output_file_path, "step", 1, 2)
    combine_student_data(folder_path, output_file_path, "heart", 1, 2)
    combine_student_data(folder_path, output_file_path, "distance", 1, 2)
    combine_student_data(folder_path, output_file_path, "sleep", 1, 2)
    combine_student_data(folder_path, output_file_path, "calories", 2, 3)
    survey.student(folder_path, output_file_path)

    reformat(output_file_path)



