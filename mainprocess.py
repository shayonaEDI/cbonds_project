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


def search_wfi(f): 
    for fe in SECURITY_FIELDS.values():
        if fe.wfi_field == f: 
            return fe.cbonds_field




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