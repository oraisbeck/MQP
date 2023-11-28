import pandas as pd
import os
import shutil


def update_csv_ts_date(csv_filename, filepath):
    """
    This function takes in a CSV filename and the filepath where the file is located.
    It reads the CSV file using pandas, extracts the year, month, and day from the filename,
    modifies the 'Time' column in the DataFrame to match the extracted date, and writes the
    modified DataFrame back to the CSV file.

    :param csv_filename: The name of the CSV file.
    :param filepath: The path where the CSV file is located.
    :return: void
    """

    if os.path.exists(filepath + csv_filename):
        df = pd.read_csv(filepath + csv_filename)
        length = len(csv_filename)

        # Better time complexity than looping through the string
        year_frm_csv = int(csv_filename[length - 12:length - 8])
        month_frm_csv = int(csv_filename[length - 8:length - 6])
        day_frm_csv = int(csv_filename[length - 6:length - 4])

        df['Time'] = pd.to_datetime(df['Time'])

        # Iterate through each datetime object in the 'Time' column and modify the year, month, and day
        df['Time'] = df['Time'].apply(lambda x: x.replace(year=year_frm_csv, month=month_frm_csv, day=day_frm_csv))

        # Write the modified DataFrame back to the CSV file
        df.to_csv(filepath + csv_filename, encoding="utf-8", index=False)
    else:
        print(
            "Error in updating the time & date of the following csv file: " + csv_filename + "located at: " + filepath)


def group_csv_by_type(directory_path, starts_with, folder_name):
    """
    Groups together a series of csv files, containing "starts_with" in their file name, in a specific directory_path

    :param directory_path: path to student directory containing csv files
    :param starts_with: feature phrase for specifying  which csv files you want to group ("calories2", "heart2", etc...)
    :param folder_name: name of the folder to contain all csv files that start with the phrase "starts_with)
    :return:
    """

    # Create a new folder called "heart"
    new_folder = os.path.join(directory_path, folder_name)
    os.makedirs(new_folder, exist_ok=True)

    # Iterate through the files in the directory
    for filename in os.listdir(directory_path):
        if filename.startswith(starts_with) and filename.endswith('.csv'):
            # Get the full path of the file
            file_path = os.path.join(directory_path, filename)
            # Move the file to the new folder
            shutil.move(file_path, new_folder)


def update_dt_csv_frm_path(csv_files_path):
    """
    This function takes a directory path where time series CSV files are stored.
    It iterates through each file in the directory and, if the file has a '.csv' extension,
    calls the 'update_csv_ts_date' function to update the date and time of that file.

    :param csv_files_path: The path of the directory where the time series CSV files are stored.
    :return: void
    """

    # Iterate through each file in the directory
    for filename in os.listdir(csv_files_path):
        if filename.endswith(".csv"):
            update_csv_ts_date(filename, csv_files_path)


def concatenate_csv_files(directory_path, concatenated_file_name):
    """
    Concatenates multiple CSV files in a directory into a single dataframe
    and saves it as a new CSV file.

    :param directory_path: Path to the directory containing the CSV files.
    :param concatenated_file_name: Name of the output CSV file.
    :return: void
    """

    # Initialize an empty list to store the dataframes
    dataframes = []

    # Iterate through each file in the directory
    for file in os.listdir(directory_path):
        # Check if the file is a CSV file
        if file.endswith(".csv"):
            # Create the file path
            file_path = os.path.join(directory_path, file)
            # Read the CSV file into a dataframe
            df = pd.read_csv(file_path)
            # Append the dataframe to the list
            dataframes.append(df)

    # Concatenate all the dataframes into a single dataframe
    if len(dataframes) != 0:
        combined_df = pd.concat(dataframes)

        # Sort the dataframe by the date column
        combined_df = combined_df.sort_values(by='Time', ascending=1)

        # Create the new directory if it doesn't already exist
        os.makedirs(directory_path + concatenated_file_name[0:len(concatenated_file_name) - 3], exist_ok=True)

        # File path to save concatenated csv file
        file_path = os.path.join(directory_path + concatenated_file_name[0:len(concatenated_file_name) - 3],
                                 concatenated_file_name)

        # Write the combined dataframe to a new CSV file
        combined_df.to_csv(file_path, index=False)


def grouped_by_month_case_pp(computer_username, term, participant, month_period):
    """
    Preprocesses the students whose data is grouped by month. Groups,
    updates the date and time, and concatenates the calories, step,
    heart, distance, and sleep files for each month period

    Note: You may have to adjust the directory file path names in the function
    to match your the location of where you store the fitbit data files

    :param computer_username: computer username necessary for path to file
    :param term: term containing students whose data is grouped by specific month
    :param participant: name of participant (ex: summernsf20.23)
    :param month_period: name of folder containing month-month data (ex: November16-December11)
    :return: void
    """
    group_csv_by_type(

        f"./{term}/{participant}/Fitbit/{month_period}",
        "heart2", f"heart_{participant}")
    group_csv_by_type(
        f"./{term}/{participant}/Fitbit/{month_period}",
        "calories2", f"cal_{participant}")
    group_csv_by_type(
        f"./{term}/{participant}/Fitbit/{month_period}",
        "step2", f"step_{participant}")
    group_csv_by_type(
        f"./{term}/{participant}/Fitbit/{month_period}",
        "sleep2", f"sleep_{participant}")
    group_csv_by_type(
        f"./{term}/{participant}/Fitbit/{month_period}",
        "distance2", f"distance_{participant}")

    update_dt_csv_frm_path(
        f"./{term}/{participant}/Fitbit/{month_period}/cal_{participant}/")
    update_dt_csv_frm_path(
        f"./{term}/{participant}/Fitbit/{month_period}/heart_{participant}/")
    update_dt_csv_frm_path(
        f"./{term}/{participant}/Fitbit/{month_period}/step_{participant}/")
    update_dt_csv_frm_path(
        f"./{term}/{participant}/Fitbit/{month_period}/sleep_{participant}/")
    update_dt_csv_frm_path(
        f"./{term}/{participant}/Fitbit/{month_period}/distance_{participant}/")

    concatenate_csv_files(
        f"./{term}/{participant}/Fitbit/{month_period}/cal_{participant}/",
        f"cal_concat_{participant}.csv")
    concatenate_csv_files(
        f"./{term}/{participant}/Fitbit/{month_period}/step_{participant}/",
        f"step_concat_{participant}.csv")
    concatenate_csv_files(
        f"./{term}/{participant}/Fitbit/{month_period}/heart_{participant}/",
        f"heart_concat_{participant}.csv")
    concatenate_csv_files(
        f"./{term}/{participant}/Fitbit/{month_period}/distance_{participant}/",
        f"distance_concat_{participant}.csv")
    concatenate_csv_files(
        f"./{term}/{participant}/Fitbit/{month_period}/sleep_{participant}/",
        f"sleep_concat_{participant}.csv")


def month_case_pp_by_term(term_name):
    """
    Performs grouped_by_month_case_pp function on a cohort of students whose data is
    grouped by month

    :param term_name: name of the term containing the data
    :return: void
    """
    for folder_student_name in os.listdir(f"./{term_name}/"):
        if os.path.isdir(f"./{term_name}/" + folder_student_name):
            for folder_name in os.listdir(f"./{term_name}/{folder_student_name}/Fitbit/"):
                if os.path.isdir(
                        f"./{term_name}/{folder_student_name}/Fitbit/" + folder_name):
                    grouped_by_month_case_pp(term_name, folder_student_name, folder_name)


def preprocess_by_term(term_path_directory):
    """
    Groups, updates, and concatenates time series fitbit
    data of each student's csv files for a particular term

    :param term_path_directory: directory path to term
    :return: void
    """
    
    for folder_name in os.listdir(term_path_directory):
        if os.path.isdir(os.path.join(term_path_directory, folder_name)):
            fitbit_data_path = term_path_directory + folder_name + "/Fitbit/"

            group_csv_by_type(fitbit_data_path, "calories2", "calories_%s" % folder_name)
            group_csv_by_type(fitbit_data_path, "distance2", "distance_%s" % folder_name)
            group_csv_by_type(fitbit_data_path, "heart2", "heart_%s" % folder_name)
            group_csv_by_type(fitbit_data_path, "step2", "step_%s" % folder_name)
            group_csv_by_type(fitbit_data_path, "sleep2", "sleep_%s" % folder_name)

            update_dt_csv_frm_path(fitbit_data_path + "/" + "calories_%s" % folder_name + "/")
            update_dt_csv_frm_path(fitbit_data_path + "/" + "distance_%s" % folder_name + "/")
            update_dt_csv_frm_path(fitbit_data_path + "/" + "heart_%s" % folder_name + "/")
            update_dt_csv_frm_path(fitbit_data_path + "/" + "step_%s" % folder_name + "/")
            update_dt_csv_frm_path(fitbit_data_path + "/" + "sleep_%s" % folder_name + "/")

            concatenate_csv_files(fitbit_data_path + "/" + "calories_%s" % folder_name + "/",
                                  "cal_concat_%s" % folder_name + ".csv")
            concatenate_csv_files(fitbit_data_path + "/" + "distance_%s" % folder_name + "/",
                                  "distance_concat_%s" % folder_name + ".csv")
            concatenate_csv_files(fitbit_data_path + "/" + "heart_%s" % folder_name + "/",
                                  "heart_concat_%s" % folder_name + ".csv")
            concatenate_csv_files(fitbit_data_path + "/" + "step_%s" % folder_name + "/",
                                  "step_concat_%s" % folder_name + ".csv")
            concatenate_csv_files(fitbit_data_path + "/" + "sleep_%s" % folder_name + "/",
                                  "sleep_concat_%s" % folder_name + ".csv")
            
