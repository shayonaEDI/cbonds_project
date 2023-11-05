#%%
import pandas as pd
import chardet #to detect encoding 
import zipfile
import shutil
import timeit
import os
import xlwings as xw #library using to read excel

#ylaifa@exchange-data.com

DATAFRAMES = {}

''' PLEASE WRITE FOLDER PATHS HERE <3 THANKS'''
WFI_FOLDER_PATH = "/Users/shayonabasu/EDI temp/WFI_FOLDER"
CBONDS_FOLDER_PATH= '/Users/shayonabasu/EDI temp/CBONDS_FOLDER/2023-09-23'

def excel_column_name(n):
    """Number to Excel-style column name, e.g., 1 = A, 26 = Z, 27 = AA, 703 = AAA."""
    name = ''
    while n > 0:
        n, r = divmod (n - 1, 26)
        name = chr(r + ord('A')) + name
    return name

'''
type(eng) 
Cbonds emissions, field is Issue Status --- we are only interested in redemption code 'REDE' 
we want to compare 
Redemption Default  in INDEFF table

WFI: INDEFF, three options: Interest default, >>redemption default (REDDFA) <<, out of default 
wfi: 
intdfa, 

CBonds emissions, 
DEFA

missing: 
if cbonds has 'redemption default', and we have BLANK

'''
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
    str_range = "A1:CU10001" #TESTING: 50,000 securities --> total size: 800,000
    values = worksheet.range(str_range).options(chunksize = 10_000).value

    df =  pd.DataFrame(values)
    df.columns = df.iloc[0] #making first row the header

    workbook.close()
    
    #returning as dataframe
    return df.tail(-1)

def read_excel_df(filename): 
    ''' NEW FUNCTION THAT CAN READ ENTIRE EXCEL FILES'''
    ''' reads entire cbonds thing, takes 70 mins'''
                    
    df = pd.read_excel(filename, header = 0, dtype = object, engine = 'openpyxl',na_values = [' ','N/A', 'NaN', 'nan', 'null', '','#N\A', '#NA','-NaN','n/a','-1.#IND','-1.#QNAN'], keep_default_na= False)

    return df

def open_cbonds_file(): 
    '''
    opening cbonds folder, and reading three files, 
    saving it in DATAFRAMES dictionairy with keys, Emitents, Default, Emissions
    '''
    files = dict()
    folder_path = CBONDS_FOLDER_PATH

    for i in os.listdir(folder_path):
        if 'emitents' in str(i): 
            files["Emitents"] = i
        if 'default' in str(i): #TO DO: default is not loading
            files['Default'] = i
        if 'emissions' in str(i):
            files['Emissions'] = i

    startt = timeit.default_timer()

    for typ, path in files.items(): 
        print('reading ', typ)
        #reading cbonds csv file
        pa = os.path.join(folder_path, path)
        df  = pd.read_csv(pa, sep = ';', header = 0, dtype = object, na_values = [' ','N/A', 'NaN', 'nan', 'null', '','#N\A', '#NA','-NaN','n/a','-1.#IND','-1.#QNAN'], keep_default_na= False, encoding = 'latin-1', on_bad_lines = 'warn', lineterminator='\n', float_precision='high',low_memory = False)

        print('finished reading ', typ, ' TIME: ', timeit.default_timer() - startt)
        #saving to dictionairy
        DATAFRAMES[typ] = df 


def open_wfi_file(): 
    '''
    Opening folder, then looping through each file other than 'FeedFormat'
    Taking the eg. 'BOND' from '20230709_BOND.txt', and 
    saving this as the key in DATAFRAMES with the value being the df
    '''
    for fi in os.listdir(WFI_FOLDER_PATH):
        if '.txt' in fi: 
            a = fi.split('_')
            name = a[1][:-4]
            pa = os.path.join(WFI_FOLDER_PATH,fi)
            print('reading file')
            startt = timeit.default_timer() 
            #note: setting dtype = object vs dtype = infer, creates a -5 second effect.. i'm leaving it as infer
            DATAFRAMES[name] = pd.read_csv(pa, header = 1, dtype = object, sep = '\t', encoding = 'latin-1', on_bad_lines = 'warn', low_memory = False)
            print('finished reading', name, 'csv file, TIME: ', timeit.default_timer() - startt)
    #old way DATAFRAMES[ "WFI bond"] = pd.read_csv(BOND_PATH, header = 1, delimiter="\t", nrows = 11, encoding = "ISO-8859-1", encoding_errors = "ignore")
   

def check_dates(wfi_date, cbond_date): 
    #wfi is a day after, ie. #SAMPLE DATES: 2023-06-24 (cbonds)
    #            : 2023-06-25 ()

    pass



''' ------------main------------'''

#open_cbonds_file()
#open_wfi_file()

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

    return {"wfi": df_wfi, "cbonds": df_cbond}
    
''' ____________________________________________________________________________'''

#%%