#%%
import pandas as pd
import chardet #to detect encoding 
''' ways to import excel data'''
import openpyxl
import os
import xlwings as xw

DATAFRAMES = {}

def excel_column_name(n):
    """Number to Excel-style column name, e.g., 1 = A, 26 = Z, 27 = AA, 703 = AAA."""
    name = ''
    while n > 0:
        n, r = divmod (n - 1, 26)
        name = chr(r + ord('A')) + name
    return name

def convert_excel_to_df(path): 
    #creating workbook file
    workbook = xw.Book(path)
    worksheet = workbook.sheets[0]
    num_col = worksheet.range('A1').end('right').column
    num_row = worksheet.range('A1').end('down').row
 
    #converting value to string 
    #str_range = "A1:" + excel_column_name(num_col) + str(num_row)
    str_range = "A1:CU50001" #TESTING: 50,000 securities --> total size: 800,000
    values = worksheet.range(str_range).options(chunksize = 10_000).value

    df =  pd.DataFrame(values)
    df.columns = df.iloc[0] #making first row the header

    workbook.close()
    
    #returning as dataframe
    return df.tail(-1)

def open_cbonds_file(): 
    '''
    opening 
    '''
    #can make file path variable
    files = dict()
    files["Emissions"] = "CBONDS Data/2023-07-08/emissions.xlsx"
    files["Emitents"] = "CBONDS Data/2023-07-08/emitents.xlsx"
    files["Default"] = "CBONDS Data/2023-07-08/default.xlsx"

    for type, path in files.items(): 
        df = convert_excel_to_df(path)
        #saving to dictionairy
        DATAFRAMES[type] = df 


def open_wfi_file(): 
    ### TERRIBLE why is it only reading 10 rows???? 
    #open

    #can make path names variable
    TESTING_PATH = "WFI Tables July"
    for fi in os.listdir(TESTING_PATH)[1:]:
        a = fi.split('_')
        name = a[1][:-4]
        pa = os.path.join(TESTING_PATH,fi)
        DATAFRAMES[name] = pd.read_csv(pa, header = 1, delimiter="\t", encoding = "ISO-8859-1", encoding_errors = "ignore", low_memory=False)
    
    
    #DATAFRAMES[ "WFI bond"] = pd.read_csv(BOND_PATH, header = 1, delimiter="\t", nrows = 11, encoding = "ISO-8859-1", encoding_errors = "ignore")
    #DATAFRAMES['WFI SCMST'] = pd.read_csv(SCMST_PATH, header = 1, delimiter="\t", nrows = 11, encoding = "ISO-8859-1", encoding_errors = "ignore")
   

open_cbonds_file()
open_wfi_file()


def check_dates(wfi_date, cbond_date): 
    #wfi is a day after, ie. #SAMPLE DATES: 2023-06-24 (cbonds)
    #            : 2023-06-25 ()
    ''' STEPS: 
    1. split the file name, then find the date type
    2. reformat the date type into a readable date
    3. check if correct
    '''
    pass



#%%