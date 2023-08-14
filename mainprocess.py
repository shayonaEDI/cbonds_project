#%%

'''
TO DO: 
  - Being able to unzip files (easy)
  - Connecting it to FTP server 
  - Automatically detecting new release
  - Internally checking correct dates for CBond and WFI datatables
'''

import pandas as pd
import os

from get_dataframes import DATAFRAMES
from comparing import SECURITY_FIELDS

MISMATCH = "mismatch"
MISSING = "missing"

''' 
copy and pasting the class for security field 
class object parameters: 
    cbonds_field: cbonds field name eg. "Currency", "CIK Number"
    cbonds_file: emmissions? Emitents? Default? 
    wfi_field: EDI WFI field name (eg. DebtCurrency)
    wfi_lookup: EDI WFI which sheet (BOND)
    match_rules: ['Exact Match', 'Requires rules']
'''
#FAIL --> look into: 
# 'Margin' and 'FRNMargin'
# 'Sukuk''DebtMarket (to be replaced)'
#  'ISIN of underlying asset',--> ISIN / ISIN RegS' 


DUMMY_DF = pd.DataFrame(data = range(0,10))

#Fields with exact matches to begin initial testing
EXACT_F = ['CFI / CFI RegS',
 'CFI 144A',
 'Currency',
 'Margin',
 'Maturity date',
 'Minimum settlement amount / Calculation amount',
 'Settlement date',
 'Subordinated debt (yes/no)',
 'Date until which the bond can be converted',
 'Price at primary placement',
 'Sukuk (yes/no)',
 'ISIN of underlying asset',
 'CIK number']

WFI_FILES = []
TESTING_PATH = "WFI Tables July" #<-- can make this global var
for fi in os.listdir(TESTING_PATH)[1:]:
    a = fi.split('_')
    name = a[1][:-4]
    WFI_FILES.append(name)

def get_cbond_df(field_name: str): 
    if field_name in SECURITY_FIELDS:
        field = SECURITY_FIELDS[field_name]
        #TODO: have to change this now because I made this exactly the same 
        if field.cbonds_file in DATAFRAMES: 
            return DATAFRAMES[field.cbonds_file]# <---- should just be this line 
        return DUMMY_DF
        '''
        if field.cbonds_file == "Emissions": 
            return DATAFRAMES["Cbond emissions"]
        elif field.cbonds_file == "Emitents": 
            return DATAFRAMES["Cbond emitents"]
        elif field.cbonds_file == "Default": 
            return DATAFRAMES["Cbond default"]
    else: 
        return DUMMY_DF'''
    #now, we have the correct cbond dataframe, 
    
def get_wfi_df(field_name: str): 
    #size times a field has multiple fields, ignoring that still
    if field_name in SECURITY_FIELDS: 
        field = SECURITY_FIELDS[field_name]
        if field.wfi_lookup in WFI_FILES: 
            return DATAFRAMES[field.wfi_lookup]
        else: 
            return DUMMY_DF

def get_dfs_field(field: str): 
    df_wfi = get_wfi_df(field_name = field)
    df_cbond = get_cbond_df(field_name = field)

    fe = SECURITY_FIELDS[field]

    return {"wfi": df_wfi['ISIN','SecID', fe.wfi_field ], "cbonds": df_cbond['ISIN / ISIN RegS', field]}
    
def get_values_for_field(field: str): 
    '''
    get LIST of values for FIELD. each item in list 
    corresponds to value for that security for the field
    '''
    dict_df = get_dfs_field(field)
    cols_c = list(dict_df['cbonds'].columns.values)
    cols_w = list(dict_df['wfi'].columns.values)
    pass

def check_wfi_row(dict_dfs: dict): 
    '''
    check by connecting thru ISIN
    check: [wfi] ISSUR IssType 
           [cbonds] Emitents CIK number --> "Exact Match"
    return true/false

    use merge: 
    df1 = DATAFRAMES['Cbond emissions'][['ISIN / ISIN RegS']]
    df2 = DATAFRAMES['BOND'][['ISIN']]
    mdf = df1.merge(df2, left_on='ISIN / ISIN RegS', right_on='ISIN')
    df4 = mdf[mdf.ISIN.notnull()]

#   print(df.iloc[-1]['Duration']) last row, col Duration

    '''

    pass

def search_wfi(f): 
    for fe in SECURITY_FIELDS.values():
        if fe.wfi_field == f: 
            return fe.cbonds_field

def get_values(): 

    dict_field_with_both_values = {}

    for field_name, field_value in SECURITY_FIELDS.items(): 
        current_dfs = get_dfs_field(field = field_name)

    pass


def is_field_present():
    #column A in CBONDS project field list is D instead of I 


    pass

'''
cbond_cols = ['ISIN / ISIN RegS']
wfi_cols = ['ISIN']

list_dfs = []

for f in exact_f: 
    fe = SECURITY_FIELDS[f]
    cbond_cols += [fe.cbonds_field]
    wfi_cols += [fe.wfi_field]

    df1 = DATAFRAMES[fe.cbonds_file][cbond_cols]
    df2 = DATAFRAMES[fe.wfi_lookup][wfi_cols]
    m_cols = list(set(cbond_cols + wfi_cols))
    mdf = df1.merge(df2, left_on='ISIN / ISIN RegS', right_on='ISIN')[m_cols]
    df3 = mdf[mdf.ISIN.notnull()]
    df3['Matching?'] = df3[fe.cbonds_field] == df3[fe.wfi_field]
    list_dfs.append(df3)

'''


''' example: 
WFI --> LOOKUP:  BOND and, FIELD:  DebtCurrency
CBONDS --> FIELD:  Emissions . and, FILE:  Currency #KEY
'''

'''
For each field in SECURITY_FIELDS: 
    - Retrieve WFI value and CBOND value for the field 
    - Retrieve correct RULE 
    - Apply RULE to (WFI value, CBOND value), and return either MISMATCH or MISSING 
    - Append to correct list 

'''

#%%