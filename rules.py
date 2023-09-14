#%%
from comparing import SECURITY_FIELDS
'''
rules.py

building a dictionairy, 
where the key is the field name of the security, 
and the value is going to be the function that builds the df
'''


#Rules_Dictionairy = {'Covered debt (yes/no)': covered_debt(mdf, fe)}

COUNTRIES = ['Andorra','United Arab Emirates','Afghanistan','Antigua and Barbuda','Albania','Armenia','Angola','Argentina','Austria','Australia','Aruba','Azerbaijan','Bosnia and Herzegovina','Barbados','Bangladesh','Belgium','Burkina Faso','Bulgaria','Bahrain','Burundi','Benin','Bermuda','Brunei','Bolivia','Brazil','Bahamas','Bhutan','Botswana','Belarus','Belize','Canada','Central African Republic','Congo','Switzerland','Cote d`Ivoire','Chile','Cameroon','China','Colombia','Costa Rica','Cape Verde','Cyprus','Czech Republic','Germany','Denmark','Dominica','Dominican Republic','Algeria','Ecuador','Estonia','Egypt','Spain','Ethiopia','Finland','Fiji','France','Gabon','United Kingdom','Grenada','Georgia','Guernsey','Ghana','Gambia','Republic of Guinea','Equatorial Guinea','Greece','Guatemala','Guinea-Bissau','Guyana','Hong Kong','Honduras','Croatia','Haiti','Hungary','Indonesia','Ireland','Israel','Isle of Man','India','Iraq','Iran','Iceland','Italy','Jersey','Jamaica','Jordan','Japan','Kenya','Kyrgyzstan','Cambodia','Saint Kitts (Christopher) and Nevis','Republic of Korea','Kuwait','Cayman Islands','Kazakhstan','Laos','Lebanon','Saint Lucia','Liechtenstein','Sri Lanka','Lesotho','Lithuania','Luxembourg','Latvia','Morocco','Monaco','Moldova','Montenegro','Madagascar','North Macedonia','Mali','Myanmar','Mongolia','Macau','Mauritania','Malta','Mauritius','Maldives','Malawi','Mexico','Malaysia','Mozambique','Namibia','Niger','Nigeria','Nicaragua','Netherlands','Norway','Nepal','New Zealand','Oman','Panama','Peru','Papua New Guinea','Philippines','Pakistan','Poland','Puerto Rico','Palestine','Portugal','Paraguay','Qatar','Romania','Serbia','Russia','Rwanda','Saudi Arabia','Solomon Islands','Seychelles','Sweden','Singapore','Slovenia','Slovakia','Sierra Leone','Senegal','Suriname','Sao Tome and Principe','El Salvador','Syria','Eswatini','Chad','Togo','Thailand','Tajikistan','Tunisia','Turkey','Trinidad and Tobago','Taiwan','Tanzania','Ukraine','Uganda','USA','Uruguay','Uzbekistan','Saint Vincent and the Grenadines','Venezuela','Virgin Islands','British','Vietnam','Samoa','Yemen','South Africa','Zambia','Zimbabwe']
COUNTRY_CODES = ['AD','AE','AF','AG','AL','AM','AO','AR','AT','AU','AW','AZ','BA','BB','BD','BE','BF','BG','BH','BI','BJ','BM','BN','BO','BR','BS','BT','BW','BY','BZ','CA','CF','CG','CH','CI','CL','CM','CN','CO','CR','CV','CY','CZ','DE','DK','DM','DO','DZ','EC','EE','EG','ES','ET','FI','FJ','FR','GA','GB','GD','GE','GG','GH','GM','GN','GQ','GR','GT','GW','GY','HK','HN','HR','HT','HU','ID','IE','IL','IM','IN','IQ','IR','IS','IT','JE','JM','JO','JP','KE','KG','KH','KN','KR','KW','KY','KZ','LA','LB','LC','LI','LK','LS','LT','LU','LV','MA','MC','MD','ME','MG','MK','ML','MM','MN','MO','MR','MT','MU','MV','MW','MX','MY','MZ','NA','NE','NG','NI','NL','NO','NP','NZ','OM','PA','PE','PG','PH','PK','PL','PR','PS','PT','PY','QA','RO','RS','RU','RW','SA','SB','SC','SE','SG','SI','SK','SL','SN','SR','ST','SV','SY','SZ','TD','TG','TH','TJ','TN','TR','TT','TW','TZ','UA','UG','US','UY','UZ','VC','VE','VG','VN','WS','YE','ZA','ZM','ZW']
CountryOfIssuer = {'Andorra': 'AD', 'United Arab Emirates': 'AE', 'Afghanistan': 'AF', 'Antigua and Barbuda': 'AG', 'Albania': 'AL', 'Armenia': 'AM', 'Angola': 'AO', 'Argentina': 'AR', 'Austria': 'AT', 'Australia': 'AU', 'Aruba': 'AW', 'Azerbaijan': 'AZ', 'Bosnia and Herzegovina': 'BA', 'Barbados': 'BB', 'Bangladesh': 'BD', 'Belgium': 'BE', 'Burkina Faso': 'BF', 'Bulgaria': 'BG', 'Bahrain': 'BH', 'Burundi': 'BI', 'Benin': 'BJ', 'Bermuda': 'BM', 'Brunei': 'BN', 'Bolivia': 'BO', 'Brazil': 'BR', 'Bahamas': 'BS', 'Bhutan': 'BT', 'Botswana': 'BW', 'Belarus': 'BY', 'Belize': 'BZ', 'Canada': 'CA', 'Central African Republic': 'CF', 'Congo': 'CG', 'Switzerland': 'CH', 'Cote d`Ivoire': 'CI', 'Chile': 'CL', 'Cameroon': 'CM', 'China': 'CN', 'Colombia': 'CO', 'Costa Rica': 'CR', 'Cape Verde': 'CV', 'Cyprus': 'CY', 'Czech Republic': 'CZ', 'Germany': 'DE', 'Denmark': 'DK', 'Dominica': 'DM', 'Dominican Republic': 'DO', 'Algeria': 'DZ', 'Ecuador': 'EC', 'Estonia': 'EE', 'Egypt': 'EG', 'Spain': 'ES', 'Ethiopia': 'ET', 'Finland': 'FI', 'Fiji': 'FJ', 'France': 'FR', 'Gabon': 'GA', 'United Kingdom': 'GB', 'Grenada': 'GD', 'Georgia': 'GE', 'Guernsey': 'GG', 'Ghana': 'GH', 'Gambia': 'GM', 'Republic of Guinea': 'GN', 'Equatorial Guinea': 'GQ', 'Greece': 'GR', 'Guatemala': 'GT', 'Guinea-Bissau': 'GW', 'Guyana': 'GY', 'Hong Kong': 'HK', 'Honduras': 'HN', 'Croatia': 'HR', 'Haiti': 'HT', 'Hungary': 'HU', 'Indonesia': 'ID', 'Ireland': 'IE', 'Israel': 'IL', 'Isle of Man': 'IM', 'India': 'IN', 'Iraq': 'IQ', 'Iran': 'IR', 'Iceland': 'IS', 'Italy': 'IT', 'Jersey': 'JE', 'Jamaica': 'JM', 'Jordan': 'JO', 'Japan': 'JP', 'Kenya': 'KE', 'Kyrgyzstan': 'KG', 'Cambodia': 'KH', 'Saint Kitts (Christopher) and Nevis': 'KN', 'Republic of Korea': 'KR', 'Kuwait': 'KW', 'Cayman Islands': 'KY', 'Kazakhstan': 'KZ', 'Laos': 'LA', 'Lebanon': 'LB', 'Saint Lucia': 'LC', 'Liechtenstein': 'LI', 'Sri Lanka': 'LK', 'Lesotho': 'LS', 'Lithuania': 'LT', 'Luxembourg': 'LU', 'Latvia': 'LV', 'Morocco': 'MA', 'Monaco': 'MC', 'Moldova': 'MD', 'Montenegro': 'ME', 'Madagascar': 'MG', 'North Macedonia': 'MK', 'Mali': 'ML', 'Myanmar': 'MM', 'Mongolia': 'MN', 'Macau': 'MO', 'Mauritania': 'MR', 'Malta': 'MT', 'Mauritius': 'MU', 'Maldives': 'MV', 'Malawi': 'MW', 'Mexico': 'MX', 'Malaysia': 'MY', 'Mozambique': 'MZ', 'Namibia': 'NA', 'Niger': 'NE', 'Nigeria': 'NG', 'Nicaragua': 'NI', 'Netherlands': 'NL', 'Norway': 'NO', 'Nepal': 'NP', 'New Zealand': 'NZ', 'Oman': 'OM', 'Panama': 'PA', 'Peru': 'PE', 'Papua New Guinea': 'PG', 'Philippines': 'PH', 'Pakistan': 'PK', 'Poland': 'PL', 'Puerto Rico': 'PR', 'Palestine': 'PS', 'Portugal': 'PT', 'Paraguay': 'PY', 'Qatar': 'QA', 'Romania': 'RO', 'Serbia': 'RS', 'Russia': 'RU', 'Rwanda': 'RW', 'Saudi Arabia': 'SA', 'Solomon Islands': 'SB', 'Seychelles': 'SC', 'Sweden': 'SE', 'Singapore': 'SG', 'Slovenia': 'SI', 'Slovakia': 'SK', 'Sierra Leone': 'SL', 'Senegal': 'SN', 'Suriname': 'SR', 'Sao Tome and Principe': 'ST', 'El Salvador': 'SV', 'Syria': 'SY', 'Eswatini': 'SZ', 'Chad': 'TD', 'Togo': 'TG', 'Thailand': 'TH', 'Tajikistan': 'TJ', 'Tunisia': 'TN', 'Turkey': 'TR', 'Trinidad and Tobago': 'TT', 'Taiwan': 'TW', 'Tanzania': 'TZ', 'Ukraine': 'UA', 'Uganda': 'UG', 'USA': 'US', 'Uruguay': 'UY', 'Uzbekistan': 'UZ', 'Saint Vincent and the Grenadines': 'VC', 'Venezuela': 'VE', 'Virgin Islands, British': 'VG', 'Vietnam': 'VN', 'Samoa': 'WS', 'Yemen': 'YE', 'South Africa': 'ZA', 'Zambia': 'ZM', 'Zimbabwe': 'ZW'}

DCCLvl1_dict = {'30/360 (30/360 ISDA)': ['30360', '30E/360', '30U360'],
 '30/360 US': ['30360', '30E/360', '30U360'],
 '30E/360': ['30360', '30E/360', '30U360'],
 '30/360 German': ['30360', '30E/360', '30U360'],
 '30E+/360': ['30360', '30E/360', '30U360'],
 'Actual/Actual (ISDA)': ['AA', 'AACA'],
 'Actual/365 (Actual/365F)': ['A365', 'A365L', 'NLA365'],
 'Actual/360': ['A360', 'NLA360'],
 'Actual/365A': ['A365', 'NLA365'],
 'Actual/365L': ['A365', 'A365L', 'NLA365'],
 'NL/365': ['A365', 'A365L', 'NLA365'],
 'Actual/364': ['A364'],
 'Actual/Actual (ICMA)': ['AA', 'AACA'],
 'BD/252': ['W252']}

def DCCLvl1(row):
    if (row["Day count convention"] == "30/360 (30/360 ISDA)" ) and (row['InterestAccrualConvention'] in ['30360', '30E/360', '30U360']):
        return 'match'
    elif row["Day count convention"] == "30/360 US" and row['InterestAccrualConvention'] in ['30360', '30E/360', '30U360']:
        return 'match'
    elif row["Day count convention"] == "30E/360" and row['InterestAccrualConvention'] in ['30360', '30E/360', '30U360']:
        return 'match'
    elif row["Day count convention"] == "30/360 German" and row['InterestAccrualConvention'] in ['30360', '30E/360', '30U360']:
        return 'match'
    elif row["Day count convention"] == "30E+/360" and row['InterestAccrualConvention'] in ['30360', '30E/360', '30U360']:
        return 'match'
    elif row["Day count convention"] == "Actual/Actual (ISDA)" and row['InterestAccrualConvention'] in ['AA', 'AACA']:
        return 'match'
    elif row["Day count convention"] == "Actual/365 (Actual/365F)" and row['InterestAccrualConvention'] in ['A365', 'A365L', 'NLA365']:
        return 'match'
    elif row["Day count convention"] == "Actual/360" and row['InterestAccrualConvention'] in ['A360', 'NLA360']:
        return 'match'
    elif row["Day count convention"] == "Actual/365A" and row['InterestAccrualConvention'] in ['A365', 'NLA365']:
        return 'match'
    elif row["Day count convention"] == "Actual/365L" and row['InterestAccrualConvention'] in ['A365', 'A365L', 'NLA365']:
        return 'match'
    elif row["Day count convention"] == "Actual/364" and row['InterestAccrualConvention'] in ['A364']:
        return 'match'
    elif row["Day count convention"] == "Actual/Actual (ICMA)" and row['InterestAccrualConvention'] in ['AA', 'AACA']:
        return 'match'
    elif row["Day count convention"] == "BD/252" and row['InterestAccrualConvention'] == 'W252':
        return 'match'
    elif row["Day count convention"] == "NL/365" and row['InterestAccrualConvention'] == 'NLA365':
        return 'match'    
    else:
        return 'mismatch'

def DCCLvl2(row):

    if row["Day count convention"] == "30E/360" and row['InterestAccrualConvention'] in ['30E/360']:
        return 'match'
    elif row["Day count convention"] == "30/360 German" and row['InterestAccrualConvention'] in ['30E/360']:
        return 'Match'
    elif row["Day count convention"] == "Actual/Actual (ISDA)" and row['InterestAccrualConvention'] in ['AA']:
        return 'match'
    elif row["Day count convention"] == "Actual/360" and row['InterestAccrualConvention'] in ['A360']:
        return 'match'
    elif row["Day count convention"] == "Actual/365L" and row['InterestAccrualConvention'] in ['A365L']:
        return 'match'
    elif row["Day count convention"] == "Actual/364" and row['InterestAccrualConvention'] in ['A364']:
        return 'match'
    elif row["Day count convention"] == "Actual/Actual (ICMA)" and row['InterestAccrualConvention'] in ['AA']:
        return 'match'
    elif row["Day count convention"] == "BD/252" and row['InterestAccrualConvention'] == 'W252':
        return 'match'
    elif row["Day count convention"] == "NL/365" and row['InterestAccrualConvention'] == 'NLA365':
        return 'match'    
    else:
        return 'mismatch'

#%%