#%%

import pandas as pd
from comparing import SECURITY_FIELDS, Field_Item
from get_dataframes import open_cbonds_file, open_wfi_file
from datetime import date
import numpy as np
from get_dataframes import DATAFRAMES
import timeit

''' _________*SORTING FIELDS INTO RULES*_________ '''

YESNO_F = ['ÑConvertable (yes/no)',
        'Subordinated debt (yes/no)',
        'Mortgage bonds (yes/no)', 
        'Structured products (yes/no)', 
        'Floating rate (yes/no)',
        'Covered debt (yes/no)',
        'Perpetual (yes/no)',
        'Placement type (eng)',
        'Securitisation',
        'pik',
        'SPV (yes/no)']

#has y,n conversion + filtering 
#can make this list by using list comprehension, condition on .match_rules
        
EXACT_F = ['CFI / CFI RegS',
 'Currency ',
 'CFI 144A',
 'ISIN of underlying asset',
 'CIK number'] 

NUMBERS = ['Integral multiple',
    'Minimum settlement amount / Calculation amount',
    'Price at primary placement',
    'Margin']

DATES_F = ['Maturity date',
        'Settlement date',
        'Date until which the bond can be converted'] #CONVT table

OTHER = [
    'Coupon frequency',
    'Country of the issuer (eng)',
    'Day count convention',
    'Payment currency',
    'Issue status (id)',
    'Bond rank (id)'] #problems

ALL =  DATES_F + YESNO_F + EXACT_F  

ALLL = OTHER + NUMBERS + ALL 

''' _________CONSTANTS_________ '''

MISMATCH = "MISMATCH"
MISSING = "MISSING"


''' _________HELPER FUNCTIONS_________'''


def get_unique_comb(mdf, fe, match_col = True):
    ''' FOR TESTING, returnings unique combinations in a df'''
    ''' 
    input:      mdf --> pd.DataFrame
                fe --> FieldObject
                match_col --> boolean (adding match type to udf)
    output:     adf --> pd.DataFrame
    '''

    if fe.cbonds_field and fe.wfi_field in list(mdf.columns.values):
        adf =  mdf.groupby([fe.cbonds_field,fe.wfi_field]).size().reset_index().rename(columns={0:'count'})
    else: 
        if mdf[fe.wfi_field + (' (EDI WFI)')].isnull().sum()>1:  
            #what is this line doing
            adf = mdf
            adf[fe.wfi_field + (' (EDI WFI)')] = adf[fe.wfi_field + (' (EDI WFI)')].astype(str)
            adf =  adf.groupby([fe.cbonds_field + ' (CBonds)',fe.wfi_field+' (EDI WFI)']).size().reset_index().rename(columns={0:'count'})
        else: 
            adf =  mdf.groupby([fe.cbonds_field + ' (CBonds)',fe.wfi_field+' (EDI WFI)']).size().reset_index().rename(columns={0:'count'})
    
    if match_col == True:
        #adding a match col of what type of match each unique combination has 
        temp_df = mdf[['Match?', fe.cbonds_field, fe.wfi_field]]
        adf = adf.merge(temp_df, on=[fe.cbonds_field, fe.wfi_field], how='inner')

    return adf

''' _________BUILDING DATAFRAME_________'''

def isins(df, f):
    '''
    reconciling isins by takeing cbonds dataframes with all three isin cols, then 
    selecting each of them, and placing into each row 
    have to 'extend' the fe.cbonds_field value into each new row created with the three isins
    then also have to implement same method to the issuer id values

    df: cbonds df
    f: cbonds field 
    '''
    isin_start = timeit.default_timer() 
    field_values = []
    isin_values = []
    iss1_values = []
    iss2_values = []
    isin_cols = ['ISIN / ISIN RegS', 'ISIN 144A', 'Isin code 3']

    if (SECURITY_FIELDS[f].cbonds_file == 'Emitents') & (SECURITY_FIELDS[f].wfi_lookup == 'ISSUR'):
        issuer_ids = ['Issuer(id)','Issuer id']
    else:
        issuer_ids = []

    print('started reconciling isins')
    # Iterate through rows in the original DataFrame 'df'
    for _, row in df.iterrows():
        field = row[f]
        # Filter non-null ISIN values and add them to the 'isin_values' array
        non_null_isins = [row[col] for col in isin_cols if not pd.isna(row[col])]
        isin_values.extend(non_null_isins)
        
        # Create an array of 'Currency' values corresponding to the number of non-null ISIN values
        field_values.extend([field] * len(non_null_isins))
        if len(issuer_ids) == 2:
            iss1 = row[issuer_ids[0]]
            iss1_values.extend([iss1] * len(non_null_isins))
            iss2 = row[issuer_ids[1]]
            iss2_values.extend([iss2] * len(non_null_isins))
    
    if len(issuer_ids) == 2:
        result_df = pd.DataFrame({f: field_values, 'Selected_ISIN': isin_values, 'Issuer(id)': iss1_values, 'Issuer id': iss2_values})
    else:
        result_df = pd.DataFrame({f: field_values, 'Selected_ISIN': isin_values})

    print('time to reconcile isins: ', timeit.default_timer()  - isin_start)
    return result_df
    

def build_merged_df(fe: Field_Item): 
    ''' 
    o---------o---------o~o---------o---------o
    Merging WFI database and CBonds database 
    o---------o---------o~o---------o---------o
    input:      fe --> FieldObject           
    output:     mdf --> pd.DataFrame
    '''

    #isin_cols = ['Selected_ISIN']
    isin_cols = ['ISIN / ISIN RegS', 'ISIN 144A', 'Isin code 3']
    cbond_cols = isin_cols + [fe.cbonds_field]

    wfi_cols = ['ISIN','SecID', fe.wfi_field]
 
    #   ACCOUNTING FOR DOUBLES (ESP PAYMENT CURRENCY)
    if fe.cbonds_field == 'Payment currency': 
        wfi_cols += ['Perpetual', fe.wfi_field ]
    elif fe.cbonds_field == 'Maturity date':
        wfi_cols += ['Perpetual']
    elif fe.cbonds_field == ['Perpetual']: 
        wfi_cols += ['MaturityDate']
    else: 
        wfi_cols = ['ISIN','SecID'] + [fe.wfi_field]
    
    mcols = list(set(wfi_cols+[fe.cbonds_field] + ['Selected_ISIN']))

    if fe.wfi_lookup == 'ISSUR':
        #Linking ISSUR --> SCMST --> Emissions
        issur_cols = ['IssID']+ [fe.wfi_field]
        scmst_cols = ['IssID','ISIN','SecID']
        ldf = DATAFRAMES['ISSUR'].loc[:,issur_cols]
        if fe.wfi_field == 'CIK':
            ldf['CIK']=ldf['CIK'].astype(int, errors = 'ignore')
        rdf = DATAFRAMES['SCMST'].loc[:,scmst_cols]
        print('merging wfi df for ISSUR')
        ldf = ldf.sort_values('IssID')
        rdf = rdf.sort_values('IssID')
        mcols += ['IssID']
        wfi_df  = pd.merge(ldf,rdf, on='IssID')
    
        if fe.cbonds_file == 'Emitents':
            emit_col = ['Issuer id',fe.cbonds_field]
            mcols += ['Issuer id']
            emis_col = ['Issuer(id)'] + isin_cols
            ldf = DATAFRAMES['Emitents'].loc[:,emit_col]
            if fe.cbonds_field == 'CIK number':
                ldf['CIK number']=ldf['CIK number'].astype(int, errors = 'ignore')
            rdf = DATAFRAMES['Emissions'].loc[:,emis_col]
            ldf = ldf.sort_values('Issuer id')
            rdf = rdf.sort_values('Issuer(id)')
            cb_df  = pd.merge(ldf,rdf, left_on='Issuer id', right_on = 'Issuer(id)')
            print('merging cbonds df for emitents')
            cb_df = isins(cb_df, fe.cbonds_field) #drops Issuer id and Issuer (id)
            cb_df = cb_df.sort_values('Selected_ISIN')
            wfi_df = wfi_df.sort_values('ISIN')
            shapes = (cb_df.shape, wfi_df.shape)
            mdf = cb_df.merge(wfi_df.query('ISIN.notnull()'), how = 'inner', left_on='Selected_ISIN', right_on='ISIN')[mcols]
            bool_s = mdf.duplicated(keep = 'first')
            print('deleting dups')
            mdf[~bool_s]
        else: 
            df1 = DATAFRAMES[fe.cbonds_file].loc[:,cbond_cols]
            df1 = isins(df1, fe.cbonds_field)
            df1 = df1.sort_values('Selected_ISIN')
            wfi_df = wfi_df.sort_values('ISIN')
            shapes = (df1.shape, wfi_df.shape)
            mdf = df1.merge(wfi_df.query('ISIN.notnull()'), left_on='Selected_ISIN', right_on='ISIN')[mcols]
    else: 
        df1 = DATAFRAMES[fe.cbonds_file].loc[:,cbond_cols] 
        df1 = isins(df1, fe.cbonds_field)
        #   ACCOUNTING FOR DOUBLES (ESP PAYMENT CURRENCY)
        if type(fe.wfi_lookup) == list and len(set(fe.wfi_lookup)) == 1:
            df2 = DATAFRAMES[fe.wfi_lookup[0]].loc[:,wfi_cols]
        else: 
            df2 = DATAFRAMES[fe.wfi_lookup].loc[:,wfi_cols]
        print('building normal mdf')
        df1 = df1.sort_values('Selected_ISIN')
        df2 = df2.sort_values('ISIN')
        shapes = (df1.shape, df2.shape)
        mdf = df1.merge(df2.query('ISIN.notnull()'), left_on='Selected_ISIN', right_on='ISIN')[mcols]
    print('finished merging, here are some stats: ', shapes, '(size of cbonds df, size of wfi df)')
    print('size of new merged df: ', mdf.shape)
    mdf = mdf[mdf.ISIN.notnull()]  #<--- this works (empties) where WFI does not have the security
    
    mdf['SecID'] == mdf['SecID'].astype(int)
    # SETTING INDEX AS SECID
    mdf.set_index('SecID', inplace = True)

    return mdf

    
def change_to_YN(mdf, fe): 
    ''' 
    changing cbonds column from 0,1's to Y,N's 
    o---------o---------oo---------o---------o
    input:  mdf: pd.DataFrame, merged data frame
            fe: FieldObject
    output: mdf: pd.DataFrame, changed inplace
    '''
    mdf[fe.cbonds_field] = mdf[fe.cbonds_field].astype(int, errors = 'ignore')
    mdf[fe.cbonds_field] = mdf[fe.cbonds_field].replace([0,1], ['N','Y'])
    return mdf

def get_colnames(fe):
    ''' return list of final arranged col names for df'''
    if type(fe.wfi_field) == list: 
        return ['Selected ISIN (CBonds)',
                fe.cbonds_field + ' (CBonds)', 'ISIN (EDI WFI)',
                fe.wfi_field[0] + ' (EDI WFI)',fe.wfi_field[1] + ' (EDI WFI)']
    if (fe.wfi_lookup == 'ISSUR') & (fe.cbonds_file == 'Emitents'): 
        return ['Selected ISIN (CBonds)',
                fe.cbonds_field + ' (CBonds)','Issuer id (EDI WFI)', 'ISIN (EDI WFI)',
                fe.wfi_field + ' (EDI WFI)','IssID (CBonds)']
    return ['Selected ISIN (CBonds)',
                fe.cbonds_field + ' (CBonds)', 'ISIN (EDI WFI)',
                fe.wfi_field + ' (EDI WFI)']

def rename_df(mdf, fe): 
    ''' IMPORTANT.. cleans it up, renames, and removes extra'''
    
    if 'Issuer id' in list(mdf.columns.values): 
        mdf = mdf.rename(columns = {"ISIN": "ISIN (EDI WFI)", fe.wfi_field: fe.wfi_field + " (EDI WFI)", 'Issuer id': 'Issuer id (EDI WFI)', 'Selected_ISIN': 'Selected ISIN (CBonds)',fe.cbonds_field: fe.cbonds_field + " (CBonds)", 'IssID': 'IssID (CBonds)'}, inplace = False)
    elif type(fe.wfi_field) == list: 
        mdf = mdf.rename(columns = {"ISIN": "ISIN (EDI WFI)", fe.wfi_field[0]: fe.wfi_field[0] + " (EDI WFI)", fe.wfi_field[1]: fe.wfi_field[1] + " (EDI WFI)",'Selected_ISIN': 'Selected ISIN (CBonds)',fe.cbonds_field: fe.cbonds_field + " (CBonds)"}, inplace = False)
    elif fe.cbonds_field == 'Margin':
        mdf = mdf.rename(columns = {"ISIN": "ISIN (EDI WFI)", fe.wfi_field: fe.wfi_field + " (EDI WFI)",'Selected_ISIN': 'Selected ISIN (CBonds)',fe.cbonds_field: fe.cbonds_field + " (CBonds)", 'Coupon rate (eng)': 'Coupon rate (eng) (CBonds)'}, inplace = False)
    else: 
        mdf = mdf.rename(columns = {"ISIN": "ISIN (EDI WFI)", fe.wfi_field: fe.wfi_field + " (EDI WFI)",'Selected_ISIN': 'Selected ISIN (CBonds)',fe.cbonds_field: fe.cbonds_field + " (CBonds)"}, inplace = False)
    
    col_names = get_colnames(fe)
    if fe.cbonds_field == "Margin": 
        col_names += ['Coupon rate (eng) (CBonds)']
    elif fe.cbonds_field == 'Payment currency': 
         col_names += ['Coupon rate (eng)', 'Perpetual']
    elif fe.cbonds_field == 'Maturity date':
        col_names += ['Perpetual']
    #i wanted to add Perpetual to something
    mdf = mdf[col_names] #THIS GETS RID OF MATCH
    
    return mdf
    
def clean_dfs(mdf, mmdf, msdf, fe, unique = True, match_col = True):
    '''
    mmdf, msdf = rename mismatch and missing dfs
    fe = field
    unique =  True (default)
    '''
    #TECHNICALLY SHOULD BE ABLE TO REMOVE THIS BOOL_S DUPLICATED?
    bool_s = mmdf.duplicated(keep = 'first')
    mmdf= mmdf[~bool_s]
    bool_s = msdf.duplicated(keep = 'first')
    msdf= msdf[~bool_s]

    if unique == False: 
        mmdf = rename_df(mmdf, fe)
        msdf = rename_df(msdf, fe)
        return {"MISMATCH": mmdf, "MISSING": msdf}
    else: 
        mmdf = rename_df(mmdf, fe)
        msdf = rename_df(msdf, fe)
        if match_col == True: 
            return {"MISMATCH": mmdf, "MISSING": msdf}

    

def run_coupon(mdf, fe): 
    mdf[fe.cbonds_field] = mdf[fe.cbonds_field].fillna(0).astype(int)
    conditions = [
        ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field].isnull() == True))]
    choices = ['missing']
    mdf['Match?'] = np.select(conditions, choices, default='n')
    msdf = mdf[mdf['Match?'] == 'missing']
    mmdf = mdf[mdf['Match?'] == 'n']

    def cpf(row):
        if row["InterestPaymentFrequency"] == "ITM" and row['Coupon frequency'] == 0:
            return 'match'
        if row["InterestPaymentFrequency"] == "ANL" and row['Coupon frequency'] == 1:
            return 'match'
        if row["InterestPaymentFrequency"] == "182" and row['Coupon frequency'] == 2:
            return 'match'
        if row["InterestPaymentFrequency"] == "SMA" and row['Coupon frequency'] == 2:
            return 'match'   
        if row["InterestPaymentFrequency"] == "180" and row['Coupon frequency'] == 2:
            return 'match'
        if row["InterestPaymentFrequency"] == "91D"  and row['Coupon frequency'] == 4:
            return 'match' 
        if row["InterestPaymentFrequency"] == "QTR" and row['Coupon frequency'] == 4:
            return 'match'    
        if row["InterestPaymentFrequency"] == "BIM" and row['Coupon frequency'] == 6:
            return 'match' 
        if row["InterestPaymentFrequency"] == "35D"and (row['Coupon frequency'] == 10 or row['InterestPaymentFrequency'] == 11 ):
            return 'match' 
        if row["InterestPaymentFrequency"] == "MNT" and (row['Coupon frequency'] == 11 or row['Coupon frequency'] == 12 or row['Coupon frequency'] == 13):
            return 'match' 
        if row["InterestPaymentFrequency"] == "28D" and (row['Coupon frequency'] == 12 or row['Coupon frequency'] == 13 or row['Coupon frequency'] == 28):
            return 'match' 
        if row["InterestPaymentFrequency"] == "WKY" and (row['Coupon frequency'] == 51 or row['Coupon frequency'] == 52 ):
            return 'match'
        else:
            return 'mismatch'
    testdf = mmdf.assign(Match=mmdf.apply(cpf, axis=1))
    #testdf.loc[(testdf['InterestPaymentFrequency']=="QTR")&(testdf['Coupon frequency']==4), 'Match'] = 'match'
    #testdf.loc[(testdf['InterestPaymentFrequency']=="28D")&(testdf['Coupon frequency']==28), 'Match'] = 'match'
    #testdf.loc[(testdf['InterestPaymentFrequency']=="182")&(testdf['Coupon frequency']==2), 'Match'] = 'match'
    mismatchdf = testdf[testdf['Match'] == 'mismatch']
    mismatchdf.drop('Match', axis = 1)
    return (mismatchdf, msdf)

def convert_date_format(date_str):
    try:
        # Convert the date string to datetime using various formats
        date = pd.to_datetime(date_str, format='%d.%m.%Y', errors='raise')
    except:
        try:
            date = pd.to_datetime(date_str, format='%Y/%m/%d', errors='raise')
        except:
            # If none of the formats match, return an empty string
            return ''
    
    if pd.notna(date):
        # Convert the datetime object to the desired format 'DD/MM/YYYY'
        return date.strftime('%d/%m/%Y')
    else:
        # If the date is NaT, return an empty string
        return ''
    

def build_df(fe):
    '''
    Calling both Building DF functions, and implementing field rules here 
    o---------o---------oo---------o---------o  
    input: fe: FieldObject
    output: dict: keys = 'MISMATCH' and 'MISSING', values: missing df, and mismatch df
    '''
    mdf = build_merged_df(fe)
    mmdf = None
    msdf = None
    #Here, changing CBonds field to make it Exact
    if fe.cbonds_field in EXACT_F: 
        conditions = [
            (mdf[fe.cbonds_field] != mdf[fe.wfi_field]) & ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field].isnull() == False)),
            ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field].isnull() == True))]
        choices = ['mismatch', 'missing']
        mdf['Match?'] = np.select(conditions, choices, default=' ')
        mmdf = mdf[mdf['Match?'] == 'mismatch']
        msdf = mdf[mdf['Match?'] == 'missing']
        bool_s = mmdf.duplicated(keep = 'first')
        mmdf= mmdf[~bool_s]
        bool_s = msdf.duplicated(keep = 'first')
        msdf= msdf[~bool_s]
        return clean_dfs(mdf, mmdf, msdf, fe)

    elif fe.cbonds_field in NUMBERS: 

        mdf[fe.cbonds_field] = mdf[fe.cbonds_field].astype('float64')
        mdf[fe.wfi_field] = mdf[fe.wfi_field].astype('float64')

        if fe.cbonds_field == 'Margin': 
            #adding Coupon rate col to Margin
            isin_cols = ['ISIN / ISIN RegS', 'ISIN 144A', 'Isin code 3']
            tomerge = DATAFRAMES['Emissions'].loc[:,['Coupon rate (eng)']+isin_cols]
            #have to prepare + reconcile ISINS move this earlier
            isin_values = []
            field_values = []
            f = 'Coupon rate (eng)'
            for _, row in tomerge.iterrows():
                field = row[f]
                # Filter non-null ISIN values and add them to the 'isin_values' array
                non_null_isins = [row[col] for col in isin_cols if not pd.isna(row[col])]
                isin_values.extend(non_null_isins)
                field_values.extend([field] * len(non_null_isins))

            result_df = pd.DataFrame({f: field_values, 'Selected_ISIN': isin_values})
            result_df = result_df.sort_values('Selected_ISIN')
            mdf = mdf.reset_index().merge(result_df, on = 'Selected_ISIN',how = 'left').set_index('SecID')
            mdf[fe.cbonds_field] = mdf[fe.cbonds_field].map('{:.5f}'.format)
            mdf[fe.wfi_field] = mdf[fe.wfi_field].map('{:.5f}'.format)
            
        else:
            mdf[fe.cbonds_field] = mdf[fe.cbonds_field].map('{:.2f}'.format)
            mdf[fe.wfi_field] = mdf[fe.wfi_field].map('{:.2f}'.format)

        cbondnotempty = mdf[mdf[fe.cbonds_field] != 'nan']
        msdf = cbondnotempty[cbondnotempty[fe.wfi_field] == 'nan']
        misma = cbondnotempty[cbondnotempty[fe.wfi_field] != 'nan']
        
        mmdf = misma[misma[fe.wfi_field]!=misma[fe.cbonds_field]]
        mmdf = rename_df(mdf = mmdf, fe = fe)

        if fe.cbonds_field == 'Price at primary placement': 
            #adding a difference col
            mmdf["Price at primary placement (CBonds)"] = mmdf["Price at primary placement (CBonds)"].astype(float)
            mmdf["PriceAsPercent (EDI WFI)"] = mmdf["PriceAsPercent (EDI WFI)"].astype(float)
            mmdf['Difference'] = mmdf['Price at primary placement (CBonds)'] - mmdf['PriceAsPercent (EDI WFI)']

        msdf = rename_df(mdf = msdf, fe = fe)

        return {"MISMATCH": mmdf, "MISSING": msdf, 'UNIQUE': get_unique_comb(mdf, fe, False)}
    
    elif fe.cbonds_field in YESNO_F: #can just change this to use the .match_rules
        mdf = change_to_YN(mdf, fe)
        if fe.cbonds_field == 'Mortgage bonds (yes/no)': 
            conditions = [
                    ((mdf[fe.cbonds_field] == 'Y') & (~mdf[fe.wfi_field].str.contains('M', na = False)) & (mdf[fe.wfi_field].isna() == False)) |
                    ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field].str.contains('M'))),
                    ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isna() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default='')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            return clean_dfs(mdf, mmdf, msdf, fe)
        elif fe.cbonds_field =='Subordinated debt (yes/no)':
            conditions = [
                (mdf[fe.cbonds_field] != mdf[fe.wfi_field]) & ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field].isnull() == False)),
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isnull() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            return clean_dfs(mdf, mmdf, msdf, fe)
        elif fe.cbonds_field == 'Сonvertable (yes/no)':
            conditions = [
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field] == 'R')) |
                ((mdf[fe.cbonds_field] == 'N') & ((mdf[fe.wfi_field] != 'R') & (mdf[fe.wfi_field].isna() == False) )),
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isna() == True))] #same
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default='')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            return clean_dfs(mdf, mmdf, msdf, fe)

        elif fe.cbonds_field == 'Covered debt (yes/no)': 
            conditions = [ #FIX THIS 
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field] != 'CB') &(mdf[fe.wfi_field].isna() == False)) |
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field] == 'CB')),
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isnull() == True))] 
            choices = ['mismatch', 'missing'] #missing should be 0 
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            return clean_dfs(mdf, mmdf, msdf, fe)

        elif fe.cbonds_field == 'Structured products (yes/no)': 
            conditions = [
                #No Missing at all - get rid of the missing rule
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field] == 'SP')) |
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field] !='SP')& (mdf[fe.wfi_field].isna() == False)),
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isnull() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            return clean_dfs(mdf, mmdf, msdf, fe)

        elif fe.cbonds_field == 'Floating rate (yes/no)':   
            conditions = [
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field].str.contains('FR'))) |
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].str.contains('FR') == False)),
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isnull() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            return clean_dfs(mdf, mmdf, msdf, fe)

        elif fe.cbonds_field == 'Perpetual (yes/no)':       
            conditions = [
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field] == 'P')) |
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field] == 'U')) |
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field] == 'I')),
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isnull() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            return clean_dfs(mdf, mmdf, msdf, fe)

        elif fe.cbonds_field == 'Placement type (eng)':  
            mdf.loc[(mdf['Placement type (eng)'] == 'Public') & (mdf['PrivatePlacement'] == 'Y'),"Match?"] = 'mismatch'
            mdf.loc[(mdf['Placement type (eng)'] == 'Private') & (mdf['PrivatePlacement'] == 'N'),"Match?"] = 'mismatch'
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            a = mdf[mdf['Placement type (eng)'] == 'Private']
            msdf = a[a['PrivatePlacement'].isna() == True]
            return clean_dfs(mdf, mmdf, msdf, fe)

        elif fe.cbonds_field == 'Securitisation': 
            conditions = [
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field].isna() == False)),
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isna() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            return clean_dfs(mdf, mmdf, msdf, fe)

        elif fe.cbonds_field == 'pik': 
            conditions = [
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field].isna() == False)),
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isna() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            return clean_dfs(mdf, mmdf, msdf, fe)

        elif fe.cbonds_field == 'SPV (yes/no)':
            conditions = [
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field] != 'SPV') & (mdf[fe.wfi_field].isna() == False)) |
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field] == 'SPV')),
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isna() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            return clean_dfs(mdf, mmdf, msdf, fe)

    elif fe.cbonds_field == 'Coupon frequency': 
        mmdf, msdf = run_coupon(mdf, fe)
        return clean_dfs(mdf, mmdf, msdf, fe)

    elif fe.cbonds_field == 'Issue status (id)': 
        #match only if 'REDDEFA'  is match
        pass
        #NUMBERS

    elif fe.cbonds_field == 'Country of the issuer (eng)': 
        from rules import CountryOfIssuer
        #namibia exception --msdf
        nam = mdf[mdf['Country of the issuer (eng)'] == 'Namibia']
        nam['CntryofIncorp'] = nam['CntryofIncorp'].astype(str)
        nami = nam[nam['CntryofIncorp']=='nan']
        condition = ~mdf.index.isin(nami.index)
        mdf = mdf[condition]

        mdf['Match?'] = [CountryOfIssuer.get(v, "n") for v in mdf['Country of the issuer (eng)']]
        mmdf = mdf[mdf['Match?'] != mdf['CntryofIncorp']] #namimbia still matched to NaN
        mmdf = mdf.drop(mdf[mdf['CntryofIncorp']=='AA'].index)
        conditions = [
            ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field].isnull() == True))]
        choices = [True]
        mdf['Missing'] = np.select(conditions, choices, default=False)
        msdf = mdf[mdf['Missing'] == True]

        return clean_dfs(mdf, mmdf, msdf, fe)

    elif fe.cbonds_field == 'Bond rank (id)': 
        mdf[fe.cbonds_field] = mdf[fe.cbonds_field].astype(int)
        if fe.wfi_field == 'SeniorJunior':
            conditions = [
                ((mdf[fe.cbonds_field] == 1) & (mdf['SeniorJunior'] == 'S')) |
                ((mdf[fe.cbonds_field] == 2) & (mdf['SeniorJunior'] == 'S')) |
                ((mdf[fe.cbonds_field] == 3) & (mdf['SeniorJunior'] == 'S')) |
                ((mdf[fe.cbonds_field] == 5) & (mdf['SeniorJunior'] == 'J')) |
                ((mdf[fe.cbonds_field] == 7) & (mdf['SeniorJunior'] == 'N')) |
                ((mdf[fe.cbonds_field] == 8) & (mdf['SeniorJunior'] == 'P')) |
                ((mdf[fe.cbonds_field] == 0)) |((mdf[fe.cbonds_field] == 4)) | ((mdf[fe.cbonds_field] == 6)),
                ((mdf[fe.cbonds_field].isna() == False) & (mdf['SeniorJunior'].isna() == True))]
        
        if fe.wfi_field == 'SecuredBy':
            mmdf, msdf = None 

        return clean_dfs(mdf, mmdf, msdf, fe)

    elif fe.cbonds_field == 'Day count convention':
        
        if fe.wfi_field == 'ConventionMethod': #1
            conditions = [
            #have to check for cases when does not contain ISDA/ICMA and we have ISDA/ICMA
            ((mdf[fe.cbonds_field].str.contains('ISDA',case = True)) & (mdf['ConventionMethod'] != 'ISDA') & (mdf['ConventionMethod'].isna() == False))|
            ((mdf[fe.cbonds_field].str.contains('ICMA',case = True)) & (mdf['ConventionMethod'] != 'ICMA') & (mdf['ConventionMethod'].isna() == False))|
            ((mdf[fe.cbonds_field].str.contains('ISDA',case = True) == False) & (mdf['ConventionMethod'] == 'ISDA'))|
            ((mdf[fe.cbonds_field].str.contains('ICMA',case = True) == False) & (mdf['ConventionMethod'] == 'ICMA')),
            #missing where CBONDS does not have ISDA & ICMA
            (((mdf[fe.cbonds_field].isna() == False) & ((mdf[fe.cbonds_field].str.contains('ISDA',case = True)) | (mdf[fe.cbonds_field].str.contains('ICMA',case = True)))) & (mdf['ConventionMethod'].isna() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mismatchdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            return clean_dfs(mdf, mmdf, msdf, fe)

        if fe.wfi_field == 'InterestAccrualConvention': #0
            from rules import DCCLvl1, DCCLvl2
            conditions = [  
                ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field].isnull() == True))]
            choices = ['missing']
            mdf['Match?'] = np.select(conditions, choices, default='n')
            msdf = mdf[mdf['Match?'] == 'missing']
            mmdf = mdf[mdf['Match?'] == 'n']
            testdf = mmdf.assign(Match=mmdf.apply(DCCLvl1, axis=1))
            mismatchdf = testdf[testdf['Match'] == 'mismatch']
            mismatchdf = mismatchdf.drop('Match', axis = 1)
            testdf1 = mmdf.assign(Match=mmdf.apply(DCCLvl2, axis=1))
            mmdf1 = testdf1[testdf1['Match'] == 'mismatch']
            mmdf1 = mmdf1.drop('Match', axis = 1)
            mmdf = rename_df(mdf = mismatchdf, fe = fe)
            mismdf1 = rename_df(mdf = mmdf1, fe = fe)
            msdf = rename_df(mdf = msdf, fe = fe)
            return {"MISMATCH LVL1": mmdf, "MISMATCH LVL2": mismdf1, "MISSING": msdf, 'Unique Combs LVL1':get_unique_comb(mismatchdf, fe),'Unique Combs LVL2':get_unique_comb(mmdf1, fe) }

    elif fe.cbonds_field == 'Payment currency': 
        conditions = [
            (mdf[fe.cbonds_field] != mdf[fe.wfi_field[0]]) & ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field[0]].isnull() == False)),
            ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field[0]].isnull() == True))]
        choices = ['mismatch', 'missing']
        mdf['Interest Match?'] = np.select(conditions, choices, default=' ')
        conditions = [
            (mdf[fe.cbonds_field] != mdf[fe.wfi_field[1]]) & ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field[1]].isnull() == False)),
            ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field[1]].isnull() == True))]
        choices = ['mismatch', 'missing']
        mdf['Maturity Match?'] = np.select(conditions, choices, default=' ')
        
        mmdf = mdf.loc[(mdf['Interest Match?'] == 'mismatch') & (mdf['Maturity Match?'] == 'mismatch')]
        msdf = mdf.loc[(mdf['Interest Match?'] == 'missing') & (mdf['Maturity Match?'] == 'missing')]
        mmdf = rename_df(mdf = mmdf, fe = fe)
        mmdf = mmdf.drop('Perpetual', axis = 1)
        msdf = rename_df(mdf = msdf, fe = fe)
        return {"MISMATCH": mmdf, "MISSING": msdf}

    elif fe.cbonds_field in DATES_F: 
        #pd.to_datetime(date_col_to_force, errors = 'coerce')
        mdf[fe.wfi_field] = mdf[fe.wfi_field].astype('str')
        mdf[fe.cbonds_field] = mdf[fe.cbonds_field].astype('str')
       #mdf[fe.wfi_field] = mdf[fe.wfi_field].str[:10]
        if mdf[fe.cbonds_field].dtype == object:    
            mdf[fe.cbonds_field] = mdf[fe.cbonds_field].str[:10]
        mdf[fe.cbonds_field] = mdf[fe.cbonds_field].str.replace('.','/')

        mdf[fe.cbonds_field] = mdf[fe.cbonds_field].apply(convert_date_format)
        mdf[fe.wfi_field] = mdf[fe.wfi_field].apply(convert_date_format)

        conditions = [
            (mdf[fe.cbonds_field] != mdf[fe.wfi_field]) & ((mdf[fe.cbonds_field] != 'NaT') & (mdf[fe.wfi_field] != 'nan')),
            ((mdf[fe.cbonds_field] != 'NaT') & (mdf[fe.wfi_field] == 'NaN'))]
        choices = ['mismatch', 'missing']
        mdf['Match?'] = np.select(conditions, choices, default=' ')
        mmdf = mdf[mdf['Match?'] == 'mismatch']
        msdf = mdf[mdf['Match?'] == 'missing']
        return clean_dfs(mdf, mmdf, msdf, fe)
    #renaming columns and dropping extra (eg. Match?)
                                                                                                                                                                                                                                                                                                                                                                                                              

''' _________EXPORTING DATAFRAME TO EXCEL_________'''

repeats = ['Currency', 'Structured products (yes/no)','Covered debt (yes/no)','Securitisation','Mortgage bonds (yes/no)', 'SPV (yes/no)']
                 
def create_file_name(fe): 
    ''' 
    ---------Specifiying file name to use field for outputted excel files---------
    input:                            
            f: string, cbonds field name #CHANGING TO fe
    output: 
            string                
    '''
    na = fe.wfi_field
    if fe.cbonds_field == 'Floating rate (yes/no)': 
        na += "_Floating"
    elif fe.cbonds_field == 'Mortgage bonds (yes/no)': 
        na += '(Mortgage_Bonds)' #filename: SecurityCharge(Mortgage_Bonds)
    elif fe.cbonds_field == 'onvertable (yes/no)':
        na = 'MaturityStructure(Convertible)'
    elif fe.cbonds_field in repeats: 
        na += '(' + fe.cbonds_field + ')'
    elif type(na) == list:  #fix
        na = 'Inerest_Maturity_Currency'
    today_date = str(date.today())
    return na.replace(" ","").replace("/","_") + "("+ today_date + ")OUTPUT11.xlsx"

def export_excel(dfs: dict,fe):
    '''
    --------- Exporting dictionairy of dfs to excel---------
    input: 
            dfs: dictionairy of dataframes
            f: string (cbonds field name)
    output: None
    '''
    #making file name
    excel_file_name = create_file_name(fe)
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

    fe = SECURITY_FIELDS[f]
    oppfe = SECURITY_FIELDS[oppf]

    path = create_file_name(oppfe)

    #reading the already created excel file back into a df
    opp_dfs = {"MISMATCH": pd.read_excel(path, sheet_name = "MISMATCH", index_col=0), 
                "MISSING": pd.read_excel(path, sheet_name = "MISSING", index_col=0)}

    #renaming the cbond field column so its the same --> working
    dfs["MISMATCH"] = dfs["MISMATCH"].rename(columns = {get_colnames(fe)[1]: f + ' and ' + get_colnames(oppfe)[1]}, inplace = False)
    dfs["MISSING"] = dfs["MISSING"].rename(columns = {get_colnames(fe)[1]: f + ' and ' + get_colnames(oppfe)[1]}, inplace = False)
    
    opp_dfs["MISMATCH"] = opp_dfs["MISMATCH"].rename(columns = {get_colnames(oppfe)[1]: f + ' and ' + get_colnames(oppfe)[1]}, inplace = False)
    opp_dfs["MISSING"] = opp_dfs["MISSING"].rename(columns = {get_colnames(oppfe)[1]: f + ' and ' + get_colnames(oppfe)[1]}, inplace = False)
    
    #concating the new dfs 
    dfConcat_MM = pd.concat([dfs["MISMATCH"], opp_dfs["MISMATCH"]], axis=0, ignore_index=False,  keys = [f, oppf])
    dfConcat_MS = pd.concat([dfs["MISSING"], opp_dfs["MISSING"]], axis=0, ignore_index=False, keys = [f, oppf])

    #adding cfi 144 to the end
    dfConcat_MM.index = dfConcat_MM.index.set_names('CFI 144A or CFI/CFI RegS', level=0)
    dfConcat_MM = dfConcat_MM.reset_index(level=('CFI 144A or CFI/CFI RegS'))
    col = dfConcat_MM.pop('CFI 144A or CFI/CFI RegS')
    dfConcat_MM.insert(4, col.name, col)

    dfConcat_MS.index = dfConcat_MS.index.set_names('CFI 144A or CFI/CFI RegS', level=0)
    dfConcat_MS = dfConcat_MS.reset_index(level=('CFI 144A or CFI/CFI RegS'))
    col = dfConcat_MS.pop('CFI 144A or CFI/CFI RegS')
    dfConcat_MS.insert(4, col.name, col)

    with pd.ExcelWriter(path, engine = "openpyxl", mode = "a", if_sheet_exists = "replace") as writer: 
        dfConcat_MM.to_excel(writer, sheet_name = "MISMATCH", index = True)
        dfConcat_MS.to_excel(writer, sheet_name = "MISSING", index = True)


''' __________________MAIN__________________'''

def run_field(field):
    #not cfi
    fe = SECURITY_FIELDS[field]
    dfs = build_df(fe)
    export_excel(dfs,fe)
    return dfs


def main():
    s = timeit.default_timer() 
    open_cbonds_file()
    print('opened cbonds file. took ', timeit.timeit())
    open_wfi_file()
    print('opened wfi file, took ', timeit.timeit())
    CFI_done = False
    
    for f in []:  
        fe = SECURITY_FIELDS[f]
        
        if type(fe.wfi_field) == list and fe.cbonds_field != 'Payment currency': 
            for i in range(len(fe.wfi_field)): 
                temp_fe = Field_Item(
                    cbonds_field = fe.cbonds_field, 
                    cbonds_file = fe.cbonds_file, 
                    wfi_field = fe.wfi_field[i], 
                    wfi_lookup = fe.wfi_lookup[i], 
                    match_rules = fe.match_rules)
                print("Have started building ", fe.wfi_field[i], "(inside loop)")
                dfs = build_df(fe = temp_fe)
                print("exporting ", temp_fe.wfi_field)
                export_excel(dfs = dfs,fe = temp_fe)
            return
        
        startt = timeit.default_timer()

        print("Have started building ", f, "and started timer")
        dfs = build_df(fe) #type(dfs): dict
        #switching
        if f == "CFI 144A" or f == 'CFI / CFI RegS': 
            CFI_done = not CFI_done

        print("Exporting ", f, " time taken to build df: ",timeit.default_timer()-startt)

        if (f == "CFI 144A" and CFI_done == False) or (f == 'CFI / CFI RegS' and CFI_done == False): 
            export_CFIs(dfs,f)
        else: 
            export_excel(dfs,fe)
    
    print('entire program took ', timeit.default_timer() - s)
    return

    
main()

#%%