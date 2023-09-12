#%%
import pandas as pd
from comparing import SECURITY_FIELDS, Field_Item
from get_dataframes import get_dfs_field, open_cbonds_file, open_wfi_file
from datetime import date
import numpy as np
from get_dataframes import DATAFRAMES
import timeit

''' _________*SORTING FIELDS INTO RULES*_________ '''

TESTFIELD = [] #example is currency

YESNO_F = ['Subordinated debt (yes/no)',
        'Mortgage bonds (yes/no)', 
        'Structured products (yes/no)', 
        'Floating rate (yes/no)',
        'Covered debt (yes/no)'
        'Perpetual (yes/no)',
        'Placement type (eng)',
        'Securitisation',
        'pik',
        'Сonvertable (yes/no)',
        'SPV (yes/no)']

#has y,n conversion + filtering 
#can make this list by using list comprehension, condition on .match_rules
        
EXACT_F = ['CFI / CFI RegS',
 'Currency',
 'CFI 144A',
 'Integral multiple',
 'Minimum settlement amount / Calculation amount',
 'ISIN of underlying asset',
 'Price at primary placement',
 'CIK number',
 'Margin',
 'Integral Multiple'] 

DATES_F = ['Maturity date',
        'Settlement date', 
        'Date until which the bond can be converted']
 
 #<----- NEXT FIELDS TO ADD

OTHER = [
    'Coupon frequency',
    'Margin',
    'Country of the issuer (eng)',
    'Bond rank (id)',
    'Day count convention',
    'Payment currency'
]

ALL =  YESNO_F + EXACT_F  + DATES_F 

ALLL = ALL + OTHER

''' _________CONSTANTS_________ '''

MISMATCH = "MISMATCH"
MISSING = "MISSING"


''' _________HELPER FUNCTIONS_________'''

def get_colnames(fe):
    ''' return list of final arranged col names for df'''
    if type(fe.wfi_field) == list: 
        return ['ISIN / ISIN RegS (CBonds)',
                fe.cbonds_field + ' (CBonds)', 'ISIN (EDI WFI)',
                fe.wfi_field[0] + ' (EDI WFI)',fe.wfi_field[1] + ' (EDI WFI)']
    if fe.wfi_lookup == 'ISSUR': 
        return ['ISIN / ISIN RegS (CBonds)',
                fe.cbonds_field + ' (CBonds)','Issuer id (EDI WFI)', 'ISIN (EDI WFI)',
                fe.wfi_field + ' (EDI WFI)','IssID (CBonds)']
    return ['ISIN / ISIN RegS (CBonds)',
                fe.cbonds_field + ' (CBonds)', 'ISIN (EDI WFI)',
                fe.wfi_field + ' (EDI WFI)']

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

def build_merged_df(fe: Field_Item): 
    ''' 
    o---------o---------oo---------o---------o
    Merging WFI database and CBonds database 
    o---------o---------oo---------o---------o
    input:      fe --> FieldObject           
    output:     mdf --> pd.DataFrame
    '''
    cbond_cols = ['ISIN / ISIN RegS'] + [fe.cbonds_field]

    if fe.cbonds_field == 'CIK number': 
        cbonds_df = DATAFRAMES['Emitents'][['CIK number']]
        cbonds_df = cbonds_df[cbonds_df['CIK number'].isnull() == False]
        wfi_df = DATAFRAMES['ISSUR'][['CIK']]
        wfi_df = wfi_df[wfi_df['CIK'].isnull() == False]
        print('building mdf for CIK')
        mdf = pd.merge(cbonds_df,wfi_df, left_on ='CIK number', right_on= 'CIK')
        return mdf
        #connecting issur and emitents
    if fe.cbonds_field == 'Bond rank (id)':
        #have to connect it with BOND/SeniorJunior / SecuredBy /Subordinate
        wfi_cols = ['ISIN','SecID'] + ['SeniorJunior']
    #   ACCOUNTING FOR DOUBLES (ESP PAYMENT CURRENCY)
    elif fe.cbonds_field == 'Payment currency': 
        wfi_cols = ['ISIN','SecID','Perpetual'] + fe.wfi_field 
    else: 
        wfi_cols = ['ISIN','SecID'] + [fe.wfi_field]
    
    mcols = list(set(wfi_cols+cbond_cols))

    if fe.wfi_lookup == 'ISSUR':
        #Linking ISSUR --> SCMST --> Emissions
    #We need to link 'IssID' in "ISSUR" table with issID in SCMST table and then we can take the ISIN from SCMST table and compare with ISIN in Cbonds.
        issur_cols = ['IssID']+ [fe.wfi_field]
        scmst_cols = ['IssID','ISIN','SecID']
        ldf = DATAFRAMES['ISSUR'][issur_cols]
        rdf = DATAFRAMES['SCMST'][scmst_cols]
        print('merging wfi df for ISSUR')
        wfi_df  = pd.merge(ldf,rdf, on='IssID')
    
        if fe.cbonds_file == 'Emitents':
            emit_col = ['Issuer id',fe.cbonds_field]
            emis_col = ['Issuer(id)','ISIN / ISIN RegS']
            ldf = DATAFRAMES['Emitents'][emit_col]
            rdf = DATAFRAMES['Emissions'][emis_col]
            cb_df  = pd.merge(ldf,rdf, left_on='Issuer id', right_on = 'Issuer(id)')
            print('merging cbonds df for emitents')
            mdf = cb_df.merge(wfi_df, how = 'inner', left_on='ISIN / ISIN RegS', right_on='ISIN')
            bool_s = mdf.duplicated(keep = 'first')
            print('deleting dups')
            mdf[~bool_s]
        else: 
            df1 = DATAFRAMES[fe.cbonds_file][cbond_cols] 
            mdf = df1.merge(wfi_df, left_on='ISIN / ISIN RegS', right_on='ISIN')[mcols]
    else: 
        df1 = DATAFRAMES[fe.cbonds_file][cbond_cols] #<---- HERE. will have to change, if converting 
        #   ACCOUNTING FOR DOUBLES (ESP PAYMENT CURRENCY)
        if type(fe.wfi_lookup) == list and len(set(fe.wfi_lookup)) == 1:
            df2 = DATAFRAMES[fe.wfi_lookup[0]][wfi_cols]
        else: 
            df2 = DATAFRAMES[fe.wfi_lookup][wfi_cols]
        #maybe I should check what changes when I do a right join insted? ie. on wfi
        print('building normal mdf')
        mdf = df1.merge(df2, left_on='ISIN / ISIN RegS', right_on='ISIN')[mcols]
    print('dropping ISINS')
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
    mdf[fe.cbonds_field] = mdf[fe.cbonds_field].replace([0.0,1.0], ['N','Y'])
    return mdf

def rename_df(mdf, fe): 
    ''' IMPORTANT.. cleans it up, renames, and removes extra'''
    
    if 'Issuer id' in list(mdf.columns.values): 
        mdf = mdf.rename(columns = {"ISIN": "ISIN (EDI WFI)", fe.wfi_field: fe.wfi_field + " (EDI WFI)", 'Issuer id': 'Issuer id (EDI WFI)', 'ISIN / ISIN RegS': 'ISIN / ISIN RegS (CBonds)',fe.cbonds_field: fe.cbonds_field + " (CBonds), 'IssID': 'IssID (CBonds)"}, inplace = False)

    elif type(fe.wfi_field) == list: 
        mdf = mdf.rename(columns = {"ISIN": "ISIN (EDI WFI)", fe.wfi_field[0]: fe.wfi_field[0] + " (EDI WFI)", fe.wfi_field[1]: fe.wfi_field[1] + " (EDI WFI)",'ISIN / ISIN RegS': 'ISIN / ISIN RegS (CBonds)',fe.cbonds_field: fe.cbonds_field + " (CBonds)"}, inplace = False)
    else: 
        mdf = mdf.rename(columns = {"ISIN": "ISIN (EDI WFI)", fe.wfi_field: fe.wfi_field + " (EDI WFI)",'ISIN / ISIN RegS': 'ISIN / ISIN RegS (CBonds)',fe.cbonds_field: fe.cbonds_field + " (CBonds)"}, inplace = False)
    
    col_names = get_colnames(fe)
    if fe.cbonds_field == "Margin": 
        col_names += ['Coupon rate (eng)']
    elif fe.cbonds_field == 'Payment currency': 
         col_names += ['Coupon rate (eng)']
    mdf = mdf['Perpetual'] #THIS GETS RID OF MATCH
    
    return mdf
    
def clean_dfs(mmdf, msdf, fe):

    bool_s = mmdf.duplicated(keep = 'first')
    mmdf= mmdf[~bool_s]
    bool_s = msdf.duplicated(keep = 'first')
    msdf= msdf[~bool_s]
    mmdf = rename_df(mmdf, fe)
    mmdf = mmdf.drop('Perpetual', axis = 1)
    msdf = rename_df(msdf, fe)

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
        if row["Coupon frequency"] == "182" and row['Coupon frequency'] == 2:
            return 'match'
        if row["InterestPaymentFrequency"] == "SMA" and row['Coupon frequency'] == 2:
            return 'match'   
        if row["InterestPaymentFrequency"] == "180" and row['Coupon frequency'] == 2:
            return 'match'
        if row["InterestPaymentFrequency"] == "91D"  and row['Coupon frequency'] == 4:
            return 'match' 
        if row["Coupon frequency"] == "QTR" and row['Coupon frequency'] == 4:
            return 'match'    
        if row["InterestPaymentFrequency"] == "BIM" and row['Coupon frequency'] == 6:
            return 'match' 
        if row["InterestPaymentFrequency"] == "35D"and (row['Coupon frequency'] == 10 or row['InterestPaymentFrequency'] == 11 ):
            return 'match' 
        if row["InterestPaymentFrequency"] == "MNT" and (row['Coupon frequency'] == 11 or row['Coupon frequency'] == 12 or row['Coupon frequency'] == 13):
            return 'match' 
        if row["InterestPaymentFrequency"] == "28D" and (row['Coupon frequency'] == 12 or row['Coupon frequency'] == 13 ):
            return 'match' 
        if row["InterestPaymentFrequency"] == "WKY" and (row['Coupon frequency'] == 51 or row['Coupon frequency'] == 52 ):
            return 'match'
        else:
            return 'mismatch'
    testdf = mmdf.assign(Match=mmdf.apply(cpf, axis=1))
    testdf.loc[(testdf['InterestPaymentFrequency']=="QTR")&(testdf['Coupon frequency']==4), 'Match'] = 'match'
    testdf.loc[(testdf['InterestPaymentFrequency']=="28D")&(testdf['Coupon frequency']==28), 'Match'] = 'match'
    testdf.loc[(testdf['InterestPaymentFrequency']=="182")&(testdf['Coupon frequency']==2), 'Match'] = 'match'
    mismatchdf = testdf[testdf['Match'] == 'mismatch']
    mismatchdf.drop('Match', axis = 1)
    return (mismatchdf, msdf)


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
        return clean_dfs(mmdf, msdf, fe)

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
            return clean_dfs(mmdf, msdf, fe)

        elif fe.cbonds_field == 'Сonvertable (yes/no)':
            conditions = [
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field] == 'R')) |
                ((mdf[fe.cbonds_field] == 'N') & ((mdf[fe.wfi_field] != 'R') |(mdf[fe.wfi_field].isna() == False) )),
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isna() == True))] #same
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default='')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            return clean_dfs(mmdf, msdf, fe)

        elif fe.cbonds_field == 'Covered debt (yes/no)': 
            conditions = [ #FIX THIS 
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field] != 'CB')) |
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field] == 'CB')),
                ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field].isnull() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            return clean_dfs(mmdf, msdf, fe)

        elif fe.cbonds_field == 'Structured products (yes/no)': 
            conditions = [
                #No Missing at all - get rid of the missing rule
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field].str.contains('SP'))) |
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].str.contains('SP') == False)),
                ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field].isnull() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            return clean_dfs(mmdf, msdf, fe)

        elif fe.cbonds_field == 'Floating rate (yes/no)':   
            conditions = [
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field].str.contains('FR'))) |
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].str.contains('FR') == False)),
                ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field].isnull() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            mmdf = rename_df(mdf = mmdf, fe = fe)
            msdf = rename_df(mdf = msdf, fe = fe)
            return {"MISMATCH": mmdf, "MISSING": msdf} 

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
            mmdf = rename_df(mdf = mmdf, fe = fe)
            msdf = rename_df(mdf = msdf, fe = fe)
            return {"MISMATCH": mmdf, "MISSING": msdf}

        elif fe.cbonds_field == 'Placement type (eng)':  
            mdf.loc[(mdf['Placement type (eng)'] == 'Public') & (mdf['PrivatePlacement'] == 'Y'),"Match"] = 'mismatch'
            mdf.loc[(mdf['Placement type (eng)'] == 'Private') & (mdf['PrivatePlacement'] == 'Y'),"Match"] = 'mismatch'
            a = mdf[mdf['Placement type (eng)'] == 'Private']
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = a[a['PrivatePlacement'].isna() == True]
            mmdf = rename_df(mdf = mmdf, fe = fe)
            msdf = rename_df(mdf = msdf, fe = fe)
            return {"MISMATCH": mmdf, "MISSING": msdf}

        elif fe.cbonds_field == 'Securitisation': 
            conditions = [
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field].isna() == False)),
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isna() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            mmdf = rename_df(mdf = mmdf, fe = fe)
            msdf = rename_df(mdf = msdf, fe = fe)
            return{"MISMATCH": mmdf, "MISSING": msdf}

        elif fe.cbonds_field == 'pik': 
            conditions = [
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field].isna() == False)),
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isna() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            mmdf = rename_df(mdf = mmdf, fe = fe)
            msdf = rename_df(mdf = msdf, fe = fe)
            {"MISMATCH": mmdf, "MISSING": msdf}

        elif fe.cbonds_field == 'SPV (yes/no)':
            conditions = [
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isna() == False) & (mdf[fe.wfi_field] != 'SPV')) |
                ((mdf[fe.cbonds_field] == 'N') & (mdf[fe.wfi_field] == 'SPV')),
                ((mdf[fe.cbonds_field] == 'Y') & (mdf[fe.wfi_field].isna() == True))]
            choices = ['mismatch', 'missing']
            mdf['Match?'] = np.select(conditions, choices, default=' ')
            mmdf = mdf[mdf['Match?'] == 'mismatch']
            msdf = mdf[mdf['Match?'] == 'missing']
            mmdf = rename_df(mdf = mmdf, fe = fe)
            msdf = rename_df(mdf = msdf, fe = fe)
            {"MISMATCH": mmdf, "MISSING": msdf}

    elif fe.cbonds_field == 'Coupon frequency': 
        mmdf, msdf = run_coupon(mdf, fe)
        mmdf = rename_df(mdf = mmdf, fe = fe)
        msdf = rename_df(mdf = msdf, fe = fe)
        return {"MISMATCH": mmdf, "MISSING": msdf}
    elif fe.cbonds_field == 'type (eng)': 
        pass
    elif fe.cbonds_field == 'Margin': 
        #adding Coupon rate (eng) column from emissions
        tomerge = DATAFRAMES['Emissions'][['Coupon rate (eng)','ISIN / ISIN RegS']]
        mdf = mdf.reset_index().merge(tomerge, on = 'ISIN / ISIN RegS',how = 'left').set_index('SecID')
        conditions = [
            (mdf[fe.cbonds_field] != mdf[fe.wfi_field]) & ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field].isnull() == False)),
            ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field].isnull() == True))]
        choices = ['mismatch', 'missing']
        mdf['Match?'] = np.select(conditions, choices, default=' ')
        mmdf = mdf[mdf['Match?'] == 'mismatch']
        msdf = mdf[mdf['Match?'] == 'missing']
        mmdf = rename_df(mdf = mmdf, fe = fe)
        msdf = rename_df(mdf = msdf, fe = fe)
        return {"MISMATCH": mmdf, "MISSING": msdf}
    elif fe.cbonds_field == 'Country of the issuer (eng)': 
        from rules import CountryOfIssuer
        mdf['Match?'] = [CountryOfIssuer.get(v, "n") for v in mdf['Country of the issuer (eng)']]
        mmdf = mdf[mdf['Match?'] == 'n']
        conditions = [
            ((mdf[fe.cbonds_field].isnull() == False) & (mdf[fe.wfi_field].isnull() == True))]
        choices = [True]
        mdf['Missing'] = np.select(conditions, choices, default=False)
        msdf = mdf[mdf['Missing'] == True]
        mmdf = rename_df(mdf = mmdf, fe = fe)
        msdf = rename_df(mdf = msdf, fe = fe)
        return {"MISMATCH": mmdf, "MISSING": msdf}
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
        if fe.wfi_field == 'MaturityCurrency': 
            pass
        mmdf = rename_df(mdf = mmdf, fe = fe)
        msdf = rename_df(mdf = msdf, fe = fe)
        return {"MISMATCH": mmdf, "MISSING": msdf}

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
        msdf = rename_df(mdf = msdf, fe = fe)
        return {"MISMATCH": mmdf, "MISSING": msdf}

    elif fe.cbonds_field in DATES_F: 
        #pd.to_datetime(date_col_to_force, errors = 'coerce')
        mdf[fe.wfi_field] = mdf[fe.wfi_field].astype('str')
        mdf[fe.wfi_field] = mdf[fe.wfi_field].str[:10]
        mdf[fe.cbonds_field] = mdf[fe.cbonds_field].astype('str')
        mdf[fe.cbonds_field] = mdf[fe.cbonds_field].str[:10]
        mdf[fe.cbonds_field] = mdf[fe.cbonds_field].str.replace('-','/')
        conditions = [
            (mdf[fe.cbonds_field] != mdf[fe.wfi_field]) & ((mdf[fe.cbonds_field] != 'NaT') & (mdf[fe.wfi_field] != 'nan')),
            ((mdf[fe.cbonds_field] != 'NaT') & (mdf[fe.wfi_field] == 'nan'))]
        choices = ['mismatch', 'missing']
        mdf['Match?'] = np.select(conditions, choices, default=' ')
        mmdf = mdf[mdf['Match?'] == 'mismatch']
        msdf = mdf[mdf['Match?'] == 'missing']
        mmdf = rename_df(mdf = mmdf, fe = fe)
        msdf = rename_df(mdf = msdf, fe = fe)
        bool_s = msdf.duplicated(keep = 'first')
        msdf[~bool_s] = msdf[~bool_s]
        #creating conditional if its the same year and month
        return {"MISMATCH": mmdf, "MISSING": msdf}
    #renaming columns and dropping extra (eg. Match?)


''' _________EXPORTING DATAFRAME TO EXCEL_________'''

repeats = ['Structured products (yes/no)','Covered debt (yes/no)','Securitisation','Mortgage bonds (yes/no)']

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
        na += '(MortgageBonds)'
    elif fe.cbonds_field in repeats: 
        na += '(' + fe.cbonds_field + ')'
    elif type(na) == list:  #fix
        na = 'IntMatCurrency'
    today_date = str(date.today())
    return na.replace(" ","").replace("/","_") + "("+ today_date + ")OUTPUT10.xlsx"

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

    #delete missing tab
    if fe.cbonds_file == 'Structured products (yes/no)': 
        del dfs['MISSING']

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

    fe = SECURITY_FIELDS[f]
    oppfe = SECURITY_FIELDS[oppf]

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

test_fields =  ['Perpetual (yes/no)','Placement type (eng)']
te = ['Day count convention']
def main():
    CFI_done = False
    
    open_cbonds_file()
    print('opened cbonds file. took ', timeit.timeit())
    open_wfi_file()
    print('opened wfi file, took ', timeit.timeit())

    for f in EXACT_F:  
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

        print("Have started building ", f, "and started timer at", timeit.timeit() )
        dfs = build_df(fe) #type(dfs): dict
        #switching
        if f == "CFI 144A" or f == 'CFI / CFI RegS': 
            CFI_done = not CFI_done

        print("Exporting ", f, " time: ", timeit.timeit())

        if (f == "CFI 144A" and CFI_done == False) or (f == 'CFI / CFI RegS' and CFI_done == False): 
            export_CFIs(dfs,f)
        else: 
            export_excel(dfs,fe)
    
    return

    
main()

#%%