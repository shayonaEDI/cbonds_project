import os
import pandas as pd

# Define paths to Cbonds
cbonds_folder = 'Cbonds'
# Function to read data from excel files in the Cbonds folder

def read_cbonds_data(folder_path):
    cbonds_data = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".xlsx"):
            file_path = os.path.join(folder_path, file_name)
            df = pd.read_excel(file_path)
            cbonds_data.append(df)
    return cbonds_data

# Get list of Dataframes from each excel file
cbonds_data_list = read_cbonds_data(cbonds_folder)
print(cbonds_data_list)