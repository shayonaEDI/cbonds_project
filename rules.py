#%%
from comparing import SECURITY_FIELDS
'''
importing rules that applies a rule DEPENDING on the field name
apply that rule here, and return the changed df ... 
--> when doing the changing, the original df is saved in SECURITY_FIELDS

--> to reduce loading speed, test.py GIVES us the already loaded df 
1. Exact
2. Mismatch dates
3. 1/0 to Yes/No
'''
EXACT = ['CFI / CFI RegS',
        'Currency',
        'CFI 144A',
        'Minimum settlement amount / Calculation amount',
        'Price at primary placement'] 

DATES = ['Maturity date',
        'Settlement date', 
        'Date until which the bond can be converted']

YESNO = ['Mortgage bonds (yes/no)', 
        'Structured products (yes/no)', 
        'Subordinated debt (yes/no)', 
        'Floating rate (yes/no)']

def has_rules(f):

    if f in EXACT: 
        return False 
    return True

def get_which_rule(f): 

    rule = []

    return rule

def applying_rule(f):
    '''
    applying rules depending on type of field, and then 
        1. converting cbond df values to same dtype as wfi 
        2. exporting the converted cbond values as a df
    '''
    df = 1

    return df

def adding_og_col_excel():
    ''' 
    Adding the original value of the col to the created excel file
    Iterates through both tabs and adds
    Has to locate the correct value by finding sec id
    '''

    pass

def date_rule(df): 


    pass

def yesno(mdf,fe):
    '''
    mdf: Being passed a MERGED dataframe where columns are 
    [secId, ISIN, wfi_field, ISIN, cbonds_field]
    fe: object 
    '''

    #MISSING
    #WFI field is NaN
    #this is kind of the subset
    missing_df = mdf.loc[(mdf[fe.wfi_field].isnull()==True) & (mdf[fe.cbonds_field].isnull() == False), ['ISIN', fe.wfi_field,'ISIN / ISIN RegS',fe.cbonds_field]]

    #MISMATCH
    mdf[fe.cbonds_field] = mdf[fe.cbonds_field].replace([0.0,1.0], ['N','Y'])

    

    return {'MISSING': missing_df}


#%%