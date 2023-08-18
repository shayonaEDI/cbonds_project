#%%
import pandas as pd
import chardet #to detect encoding 
import zipfile
import shutil
import os
import xlwings as xw #library using to read excel

DATAFRAMES = {}

''' PLEASE WRITE FOLDER PATHS HERE <3 THANKS'''
CBONDS_FOLDER_PATH = "/Users/shayonabasu/Downloads/EDI Summer 23/cbonds main project/CBONDS Data/2023-07-08"

WFI_FOLDER_PATH = "/Users/shayonabasu/Downloads/EDI Summer 23/cbonds main project/WFI Tables July"

def excel_column_name(n):
    """Number to Excel-style column name, e.g., 1 = A, 26 = Z, 27 = AA, 703 = AAA."""
    name = ''
    while n > 0:
        n, r = divmod (n - 1, 26)
        name = chr(r + ord('A')) + name
    return name

def convert_excel_to_df(path): 
    '''
    DEPRECATED --- OLD FUNC TO READ 50,000 ROWS 
    -----
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

def read_excel_df(filename): 
    ''' NEW FUNCTION THAT CAN READ ENTIRE EXCEL FILES'''
    ''' reads entire cbonds thing, takes 70 mins'''
    df = pd.read_excel(filename)
    return df

def open_cbonds_file(): 
    '''
    opening cbonds folder, and reading three files, 
    saving it in DATAFRAMES dictionairy with keys, Emitents, Default, Emissions
    '''
    files = dict()
    folder_path = CBONDS_FOLDER_PATH
    
    for i in os.listdir(folder_path):
        if i[:-5] == 'emitents': 
                files["Emitents"] = i
        if i[:5] == 'default': 
            files['Default'] = i
        if i[:-5] == 'emissions':
            files['Emissions'] = i

    for type, path in files.items(): 
        df = convert_excel_to_df(os.path.join(folder_path,path))
        #saving to dictionairy
        DATAFRAMES[type] = df 


def open_wfi_file(): 
    '''
    Opening folder, then looping through each file other than 'FeedFormat'
    Taking the eg. 'BOND' from '20230709_BOND.txt', and 
    saving this as the key in DATAFRAMES with the value being the df
    '''
    for fi in os.listdir(WFI_FOLDER_PATH)[1:]:
        a = fi.split('_')
        name = a[1][:-4]
        pa = os.path.join(WFI_FOLDER_PATH,fi)
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


'''_____________Helper functions to return df based on type_____________'''
DUMMY_DF = pd.DataFrame(data = range(0,10))

def get_cbond_df(field): #field type is Field_Item
    ''' returns df depening on security bond type'''
    if field.cbonds_file in DATAFRAMES: 
        return DATAFRAMES[field.cbonds_file]# <---- should just be this line 
    return DUMMY_DF
    
WFI_FILES = []
''' adding wfi field name to a list '''
for fi in os.listdir(WFI_FOLDER_PATH)[1:]:
    name = fi.split('_')
    WFI_FILES.append(name[1][:-4])

def get_wfi_df(field):  
    ''' returning df depending on security wfi lookup'''
    if field.wfi_lookup in WFI_FILES: 
        return DATAFRAMES[field.wfi_lookup]
    else: 
        return DUMMY_DF

def get_dfs_field(field): 
    ''' returning relevant df in dict type based on field type'''
    df_wfi = get_wfi_df(field = field) #field type is Field_Item
    df_cbond = get_cbond_df(field = field) #field type is Field_Item

    return {"wfi": df_wfi[['ISIN','SecID', field.wfi_field]], "cbonds": df_cbond[['ISIN / ISIN RegS', field.cbonds_field]]}
    
''' ______________________________________________________________________________'''

#%%