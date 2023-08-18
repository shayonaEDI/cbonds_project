#%%
import pandas as pd
from comparing import SECURITY_FIELDS
from get_dataframes import get_dfs_field 
from datetime import date
from get_dataframes import DATAFRAMES

''' _________*SORTING FIELDS INTO RULES*_________ '''

TESTFIELD = ['Mortgage bonds (yes/no)'] #example is currency


FILTER_F = ['Mortgage bonds (yes/no)', 
        'Structured products (yes/no)', 
        'Floating rate (yes/no)']

YESNO_F = ['Subordinated debt (yes/no)'] + FILTER_F 
#has y,n conversion + filtering 
        
EXACT_F = ['CFI / CFI RegS',
'Currency',
 'CFI 144A',
 'Minimum settlement amount / Calculation amount',
 'Price at primary placement'] 

DATES_F = ['Maturity date',
        'Settlement date', 
        'Date until which the bond can be converted']

NEW_F = ['Perpetual (yes/no)', #<----- NEXT FIELDS TO ADD
'Placement Type (eng) ',
'Coupon Frequency']

ALL =  YESNO_F + EXACT_F  + DATES_F

''' _________CONSTANTS_________ '''

MISMATCH = "MISMATCH"
MISSING = "MISSING"



''' _________HELPER FUNCTIONS_________'''

def get_colnames(f):
    ''' return list of final arranged col names for df'''
    fe = SECURITY_FIELDS[f]
    return ['ISIN (EDI WFI)',
                fe.wfi_field + ' (EDI WFI)',
                'ISIN / ISIN RegS (CBonds)',
                fe.cbonds_field + ' (CBonds)']

def get_unique_comb(mdf, fe):
    ''' FOR TESTING, returnings unique combinations in a df'''
    ''' 
    input:      mdf --> pd.DataFrame
                fe --> FieldObject
    output:     adf --> pd.DataFrame
    '''
    adf = mdf.groupby([fe.cbonds_field,fe.wfi_field]).size().reset_index().rename(columns={0:'count'})
    return adf
    
''' _________BUILDING DATAFRAME_________'''

def build_merged_df(fe): 
    ''' 
    --------- Merging WFI database and CBonds database ---------

    input:      fe --> FieldObject           
    output:     mdf --> pd.DataFrame
    '''
    dfs = get_dfs_field(fe) #returns dict
    cbond_cols = ['ISIN / ISIN RegS'] + [fe.cbonds_field]
    wfi_cols = ['ISIN','SecID'] + [fe.wfi_field]

    #building comparision tables
    df1 = dfs['cbonds'][cbond_cols] #<---- HERE. will have to change, if converting 
    df2 = dfs['wfi'][wfi_cols]
    
    mcols = list(set(wfi_cols+cbond_cols))
    mdf = df1.merge(df2, left_on='ISIN / ISIN RegS', right_on='ISIN')[mcols]
    mdf = mdf[mdf.ISIN.notnull()]  #<--- this works (empties) where WFI does not have the security
    # SETTING INDEX AS SECID
    mdf.set_index('SecID', inplace = True)

    return mdf

def build_missing_df(fe, mdf): 
    ''' 
    ---------Building Missing Dataframe with following rules ---------
            1. CBonds: Any Value, WFI: BLANK (done)
            2. CBonds: Value, WFI: 'Unknown' ( to do )
            3. CBonds: Specific Value, WFI: BLANK ( to do )
            4. No Missing df, so return empty ( to do )

    input:      fe --> FieldObject           
    output:     mdf --> pd.DataFrame
    '''
    col_names = get_colnames(fe.cbonds_field)
    
    #RULE ONE: Creating Missing  -- WFI is empty, but CBONDS is not empty
    missing_df = mdf.loc[(mdf[fe.wfi_field].isnull()==True) & (mdf[fe.cbonds_field].isnull() == False), ['ISIN', fe.wfi_field,'ISIN / ISIN RegS',fe.cbonds_field]]
    missing_df = missing_df.dropna(subset=['ISIN'])
    missing_df = missing_df.rename(columns = {"ISIN": "ISIN (EDI WFI)", fe.wfi_field: fe.wfi_field + " (EDI WFI)",'ISIN / ISIN RegS': 'ISIN / ISIN RegS (CBonds)',fe.cbonds_field: fe.cbonds_field + " (CBonds)"}, inplace = False)
    missing_df = missing_df[col_names]

    return missing_df
    

def build_mismatch_df(fe, mdf: pd.DataFrame, col: str = None): 
    ''' 
    --------- Building Mismatch Dataframe---------

    inputs: fe --> FieldObject
            mdf --> pd.DataFrame
            col --> string (Column name to compare against), --> may change <-- 
    output: mismatchdf --> pd.Dataframe
            
    '''    
    #cleaning up dfs <--- check not loosing any imp information (after new comments)
    df3 = mdf[mdf[fe.wfi_field].notnull()]  # getting rid of all WFI missing rows --> or rows that owuld be in missing sheet
    df3 = df3[df3[fe.cbonds_field].notnull()]  # getting rid of all WFI missing rows --> or rows that owuld be in missing sheet

    if not col: 
        df3['Matching?'] = (df3[fe.cbonds_field] == df3[fe.wfi_field]).astype(bool)
    else: 
        df3['Matching?'] = (df3[fe.cbonds_field] == df3[col]).astype(bool)
        df3 = df3.drop(labels = col, axis = 1, inplace = False)
   
    df3 = df3.rename(columns = {"ISIN": "ISIN (EDI WFI)", fe.wfi_field: fe.wfi_field + " (EDI WFI)",'ISIN / ISIN RegS': 'ISIN / ISIN RegS (CBonds)',fe.cbonds_field: fe.cbonds_field + " (CBonds)"}, inplace = False)

    mismatchdf = df3.loc[df3['Matching?']==False]
    mismatchdf = mismatchdf.drop(labels = "Matching?", axis = 1, inplace = False)
    mismatchdf = mismatchdf[get_colnames(fe.cbonds_field)]

    return mismatchdf

def apply_filter(filter: dict, mdf, fe, strict = False): 
    ''' 
    --------- Apply filter, and return ---------
        
    input:  filter: dictionairy, containing key: WFI value, that connects to value: CBonds value
            mdf: pd.DataFrame, merged data frame
            fe: FieldObject
            strict: DEFAULT --> False, if ONLY contains or 
    output: mdf: pd.DataFrame, changed inplace
    '''
    if strict == False: 
        if 'N' not in list(filter.values()): 
            mdf.loc[~mdf[fe.wfi_field].str.contains(list(filter.keys())[0], na=False), 'Isin'] = 'N'
        for wfi_val, cbonds_val in filter.items(): 
            mdf.loc[mdf[fe.wfi_field].str.contains(wfi_val, na=False), 'Isin'] = cbonds_val

    else: 
        if 'N' not in list(filter.values()):
            mdf.loc[mdf[fe.wfi_field] != list(filter.keys())[0], "Isin"] = "N"
        for wfi_val, cbonds_val in filter.items(): 
            mdf.loc[mdf[fe.wfi_field] == wfi_val, "Isin"] = cbonds_val

    return mdf
    
def change_to_YN(mdf, fe): 
    ''' 
    --------- changing cbonds column from 0,1's to Y,N's ---------
    input:  mdf: pd.DataFrame, merged data frame
            fe: FieldObject
    output: mdf: pd.DataFrame, changed inplace
    '''

    mdf[fe.cbonds_field] = mdf[fe.cbonds_field].replace([0.0,1.0], ['N','Y'])

    return mdf

def build_df(fe):
    '''
    --------- Calling both Building DF functions, and implmenenting field rules here ---------
    
    input: fe: FieldObject
    output: dict: keys = 'MISMATCH' and 'MISSING', values: missing df, and mismatch df
    '''
    mdf = build_merged_df(fe)
    print('Have built the merged df of size ', mdf.shape)
    msdf = build_missing_df(fe, mdf)

    #Here, changing CBonds field to make it Exact
    if fe.cbonds_field in YESNO_F: 
        mdf = change_to_YN(mdf, fe)
        msdf = build_missing_df(fe, mdf)
        print('Converted CBonds 1 and 0s to Y, Ns')
        print(mdf.head(2).to_string())
        if fe.cbonds_field == "Mortgage bonds (yes/no)": 
            mdf = apply_filter(filter = {"M":'Y'}, mdf = mdf, fe = fe)
            mmdf = build_mismatch_df(fe = fe, mdf = mdf, col = 'Isin')
        elif fe.cbonds_field == "Structured products (yes/no)": 
            mdf = apply_filter(filter = {"SP":'Y'}, mdf = mdf, fe = fe, strict = True)
            mmdf = build_mismatch_df(fe, mdf, col = 'Isin')
        elif fe.cbonds_field == "Floating rate (yes/no)": 
            mdf = apply_filter(filter = {"FR":'Y'}, mdf = mdf, fe = fe)
            mmdf = build_mismatch_df(fe, mdf, col = 'Isin')
        elif fe.cbonds_field == 'Perpetual (yes/no)': 
            mdf = apply_filter(filter = {'P':'Y', 'U':'Y', 'I':'Y', None: 'N'}, mdf = mdf, fe = fe)
            mmdf = build_mismatch_df(fe, mdf, col = 'Isin')
        elif fe.cbonds_field == 'Placement Type (eng)': 
            mdf = apply_filter(filter = {'Public': None, 'Public': 'N', 'Private': 'Y'}, mdf = mdf, fe = fe)
        else: mmdf = build_mismatch_df(fe, mdf)
    elif fe.cbonds_field in DATES_F: 
        mdf[fe.cbonds_field] = mdf[fe.cbonds_field].astype('datetime64')
        mdf[fe.cbonds_field] = mdf[fe.cbonds_field].dt.normalize()
        mdf[fe.wfi_field] = mdf[fe.wfi_field].astype('datetime64')
        mdf[fe.wfi_field] = mdf[fe.wfi_field].dt.normalize()
        mmdf = build_mismatch_df(fe, mdf)
    else: 
        mmdf = build_mismatch_df(fe, mdf)
   
    return {"MISMATCH": mmdf, "MISSING": msdf}


''' _________EXPORTING DATAFRAME TO EXCEL_________'''


def create_file_name(f): 
    ''' 
    ---------Specifiying file name to use field for outputted excel files---------
    input: 
            f: string, cbonds field name
    output: 
            string 
    '''
    na = SECURITY_FIELDS[f].wfi_field
    today_date = str(date.today())
    return na.replace(" ","").replace("/","_") + "("+ today_date + ")ALL.xlsx"

def export_excel(dfs,f):
    '''
    --------- Exporting dictionairy of dfs to excel---------
    input: 
            dfs: dictionairy of dataframes
            f: string (cbonds field name)
    output: None
    '''
    #making file name
    excel_file_name = create_file_name(f)
    writer = pd.ExcelWriter(excel_file_name, engine = 'xlsxwriter')

    for sheetname, df in dfs.items(): #look through 'dict' of dataframes
        df.to_excel(writer, sheet_name = sheetname) #sending df to writer
        #adjusting col width dynamically
        worksheet = writer.sheets[sheetname]
        nlevs = df.index.nlevels
        for idx, col in enumerate(df): #looping thru the cols --> test
            series = df[col]
            max_len = max((series.astype(str).map(len).max(),#len of largest value in col
                    len(str(series.name)) # len of column nae
                    )) + 1 #adding extra space
            worksheet.set_column(idx + nlevs, idx + nlevs, max_len) #set col width
    writer.close()


def export_CFIs(dfs, f): 
    '''
     --------- Concatting second CFI field to already made first CFI excel---------
    input: 
            dfs: dictionairy of dataframes
            f: string (cbonds field name)
    output: None
    '''
    if f == "CFI 144A":   #first getting opposite cfi
        oppf = 'CFI / CFI RegS'
    elif f == 'CFI / CFI RegS': 
        oppf = 'CFI 144A'

    path = create_file_name(oppf)

    #reading the already created excel file back into a df
    opp_dfs = {"MISMATCH": pd.read_excel(path, sheet_name = "MISMATCH", index_col=0), 
                "MISSING": pd.read_excel(path, sheet_name = "MISSING", index_col=0)}

    #renaming the cbond field column so its the same --> working
    dfs["MISMATCH"] = dfs["MISMATCH"].rename(columns = {get_colnames(f)[-1]: f + " and " + oppf + " (Cbonds)"}, inplace = False)
    dfs["MISSING"] = dfs["MISSING"].rename(columns = {get_colnames(f)[-1]: f + " and " + oppf + " (Cbonds)"}, inplace = False)
    
    opp_dfs["MISMATCH"] = opp_dfs["MISMATCH"].rename(columns = {get_colnames(oppf)[-1]: f + " and " + oppf + " (Cbonds)"}, inplace = False)
    opp_dfs["MISSING"] = opp_dfs["MISSING"].rename(columns = {get_colnames(oppf)[-1]: f + " and " + oppf + " (Cbonds)"}, inplace = False)
    
    #concating the new dfs 
    dfConcat_MM = pd.concat([dfs["MISMATCH"], opp_dfs["MISMATCH"]], axis=0, ignore_index=False,  keys = [f, oppf])
    dfConcat_MS = pd.concat([dfs["MISSING"], opp_dfs["MISSING"]], axis=0, ignore_index=False, keys = [f, oppf])

    with pd.ExcelWriter(path, engine = "openpyxl", mode = "a", if_sheet_exists = "replace") as writer: 
        dfConcat_MM.to_excel(writer, sheet_name = "MISMATCH", index = True)
        dfConcat_MS.to_excel(writer, sheet_name = "MISSING", index = True)


''' __________________MAIN__________________'''

def main():
    CFI_done = False
    
    for f in ALL:  
        fe = SECURITY_FIELDS[f]
        print('Starting test with: ', fe.cbonds_field, 'and ',fe.wfi_field)
        #replace everything with fe? 
        dfs = build_df(fe) #type(dfs): dict

        #switching
        if f == "CFI 144A" or f == 'CFI / CFI RegS': 
            CFI_done = not CFI_done

        if (f == "CFI 144A" and CFI_done == False) or (f == 'CFI / CFI RegS' and CFI_done == False): 
            export_CFIs(dfs,f)
        else: 
            print('exporting excel')
            export_excel(dfs,f)
        

        print("exporting ", f)
    
main()

#%%