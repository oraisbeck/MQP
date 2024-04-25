# starting over
import Preprocess
import survey
from Preprocess import run_preprocessing
import os
from Process import run_all
import pandas as pd

file_path = "/Users/oliviaraisbeck/Downloads/MQP"


def combine(file_path):
    final_data = pd.DataFrame()

    for file in os.listdir(file_path):
        if file.lower().endswith('.csv'):
            file_data = pd.read_csv(os.path.join(file_path, file))

            if final_data.empty:
                final_data = file_data
            else:
                final_data = pd.merge(final_data, file_data, on='Date', how='outer',
                                      suffixes=('', f'_{os.path.splitext(file)[0]}'))

    # Convert 'Date' to datetime for proper sorting
    if 'Date' in final_data.columns:
        # Sort the DataFrame by the 'Date' column
        final_data.sort_values(by='Date', inplace=True)

        # Reset the index
        final_data.reset_index(drop=True, inplace=True)

        # Save the updated DataFrame to a new CSV file
        final_data.to_csv(os.path.join(file_path, "all.csv"), index=False)


def process(file_path):
    for cohort in os.listdir(file_path):
        if not (cohort == '.git'):  # registers .git as a directory if it is in there
            if os.path.isdir(os.path.join(file_path, cohort)):
                cohort_path = file_path + "/" + cohort
                run_preprocessing(cohort_path)
                run_all(cohort_path, file_path + "/" + cohort + ".csv")
    combine(file_path)


# data = pd.DataFrame()
# survey.student(file_path + "/E1 term" , data)

process(file_path)
