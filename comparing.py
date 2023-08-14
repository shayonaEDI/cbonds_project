'''
comparing.py
'''

#%%
import pandas as pd 

''' 
create object for matches

class object: 
    cbonds_field: cbonds field name
    cbonds_file: emmissions? Emitents? Default?
    wfi_field: EDI WFI field name (eg. DebtCurrency)
    wfi_lookup: EDI WFI which sheet (BOND)
    match_rules: exact match, requires rules
'''

SECURITY_FIELDS = {}

class Field_Item: 
    def __init__(self, cbonds_field = None, cbonds_file = None, wfi_field = None, wfi_lookup = None, match_rules = None, rule_type = None): 
        self.cbonds_field = cbonds_field
        self.cbonds_file = cbonds_file
        self.wfi_field = wfi_field
        self.wfi_lookup = wfi_lookup
        self.match_rules = match_rules
        self.rule_type = rule_type
        
''' example: 
WFI --> LOOKUP:  BOND and, FIELD:  DebtCurrency
CBONDS --> FIELD:  Emissions . and, FILE:  Currency
'''

def create_object(field): 
    ''' where field names is a iterator of all the field names'''
    new_obj = Field_Item()
    #Emissions, Amortizing security (yes/no)
    new_obj.cbonds_field = field[2]
    new_obj.cbonds_file = field[1]
    
    #Splitting BOND/DebtCurrency
    splitted = field[4].split("/")
    if len(splitted) == 2: 
        new_obj.wfi_field = splitted[1]     
        new_obj.wfi_lookup = splitted[0]
    else: 
        new_obj.wfi_field = splitted[-1]     
        new_obj.wfi_lookup = splitted[0]
    
    new_obj.match_rules = field[5]
    #saving field to 
    SECURITY_FIELDS[new_obj.cbonds_field] = new_obj

    return new_obj

#can create this as class to pass thru any field names, 
#but right now is a given constant field

#MAIN()
df_field_list = pd.read_csv('Connecting_Field.csv')
for f in df_field_list.itertuples(): 
    create_object(field= f)

    


#%%