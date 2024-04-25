# This code is for taking the raw datafiles, and formatting which files we want and in the form we want them in

import os
import shutil


def extract_months(path):
    """
        This function takes in a directory path which should contain the students files to be processed.
        It loops through the student folders and goes into the Fitbit file of each student.
        If the students csv files are sorted into months, it extracts the files and puts them into just the fitbit folder.
        The now empty month folders are then deleted.

        :param path: The directory of all the student files to look through
        :return: void
    """
    for folder_student_name in os.listdir(path):
        if not (folder_student_name == '.git'):  # registers .git as a directory if it is in there
            if os.path.isdir(os.path.join(path, folder_student_name)):
                for folder_name in os.listdir(path + "/" + folder_student_name + "/Fitbit/"):
                    if os.path.isdir(path + "/" + folder_student_name + "/Fitbit/" + folder_name + "/"):
                        month_path = path + "/" + folder_student_name + "/Fitbit/" + folder_name + "/"
                        for filename in os.listdir(month_path):
                            file_path = os.path.join(month_path, filename)
                            new_path = path + "/" + folder_student_name + "/Fitbit/"
                            shutil.move(file_path, new_path)  # move out of month folder into fitbit folder
                        os.rmdir(month_path)  # delete empty month folder


def delete_unused(path):
    """
           This function takes in a directory path which should contain the students files to be processed.
           It loops through the student folders and goes into the Fitbit file of each student.
           It finds all the summary (heart, activies, steps) files and deletes them.

           :param path: The directory of all the student files to look through
           :return: void
   """
    for folder_name in os.listdir(path):
        if os.path.isdir(os.path.join(path, folder_name)):
            fitbit_data_path = path + "/" + folder_name + "/Fitbit/"
            files = os.listdir(fitbit_data_path)
            heartsummeray_files = [file for file in files if
                                   (file.lower().startswith('heartsummary') and file.lower().endswith('.csv')
                                    or file.lower().startswith('activiessummary') and file.lower().endswith('.csv')
                                    or file.lower().startswith('sleepsummary') and file.lower().endswith('.csv')
                                    or file.lower().startswith('stepssummary') and file.lower().endswith('.csv'))]

            for file in heartsummeray_files:
                file_path = os.path.join(path + "/" + folder_name + "/Fitbit/", file)
                try:
                    os.remove(file_path)
                    print(f"Deleted file: {file}")
                except Exception as e:
                    print(f"Error deleting file {file}: {e}")


def delete_empty(path):
    """
           This function takes in a directory path which should contain the students files to be processed.
           It loops through the student folders and goes into the Fitbit file of each student.
           If there are no csv files inside the Fitbit folder, the entire student's folder get's deleted

           :param path: The directory of all the student files to look through
           :return: void
   """
    for folder_name in os.listdir(path):
        if os.path.isdir(os.path.join(path, folder_name)):
            fitbit_data_path = path + "/" + folder_name + "/Fitbit/"
            files = os.listdir(fitbit_data_path)
            csv_files = [file for file in files if file.lower().endswith('.csv')]
            if not csv_files:
                try:
                    shutil.rmtree(path + "/" + folder_name)
                    print(f"Deleted Folder: {folder_name}")
                except Exception as e:
                    print(f"Error deleting folder: {e}")


def run_preprocessing(path):
    """
           Run all three methods to only keep students with non-summary csv files in the path

           :param path: The directory of all the student files to look through
           :return: void
   """
    extract_months(path)
    delete_unused(path)
    delete_empty(path)
