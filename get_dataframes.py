#%%
import pandas as pd
import chardet #to detect encoding 
import zipfile
import shutil
import os
import xlwings as xw #library using to read excel

DATAFRAMES = {}

def excel_column_name(n):
    """Number to Excel-style column name, e.g., 1 = A, 26 = Z, 27 = AA, 703 = AAA."""
    name = ''
    while n > 0:
        n, r = divmod (n - 1, 26)
        name = chr(r + ord('A')) + name
    return name

def convert_excel_to_df(path): 
    '''
    using xlwings to read the excel into a readable format, that converts to a pd
    all this drama with the size of the excel is here
    ----- 
    input: file path 
    output: dataframe
    '''
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
    folder_path = "/Users/shayonabasu/Downloads/EDI Summer 23/cbonds main project/CBONDS Data/2023-07-08"
    
    for i in os.listdir(folder_path):
        if i[:-5] == 'emitents': 
                files["Emissions"] = i
        if i[:5] == 'default': 
            files['Default'] = i
        if i[:-5] == 'emissions':
            files['Emissions'] = i

    for type, path in files.items(): 
        df = convert_excel_to_df(path)
        #saving to dictionairy
        DATAFRAMES[type] = df 


def open_wfi_file(): 
    '''
    Opening folder, then looping through each file other than 'FeedFormat'
    Taking the eg. 'BOND' from '20230709_BOND.txt', and 
    saving this as the key in DATAFRAMES with the value being the df
    '''
    TESTING_PATH = "/Users/shayonabasu/Downloads/EDI Summer 23/cbonds main project/WFI Tables July"
    for fi in os.listdir(TESTING_PATH)[1:]:
        a = fi.split('_')
        name = a[1][:-4]
        pa = os.path.join(TESTING_PATH,fi)
        DATAFRAMES[name] = pd.read_csv(pa, header = 1, delimiter="\t", encoding = "ISO-8859-1", encoding_errors = "ignore", low_memory=False)
    
    
    #old way DATAFRAMES[ "WFI bond"] = pd.read_csv(BOND_PATH, header = 1, delimiter="\t", nrows = 11, encoding = "ISO-8859-1", encoding_errors = "ignore")
   
   
def check_dates(wfi_date, cbond_date): 
    #wfi is a day after, ie. #SAMPLE DATES: 2023-06-24 (cbonds)
    #            : 2023-06-25 ()

    pass



''' ------------main------------'''

open_cbonds_file()
open_wfi_file()

''' ----------------------------'''



#%%