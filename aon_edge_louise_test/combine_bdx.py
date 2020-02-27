# %% import packages

import pandas as pd
import glob
import xlrd
from sql_connection import sql_connection


# %% find files

filepath_premium = r'\\svrtcs04\Syndicate Data\Actuarial\Pricing\2_Account_Pricing\NFS_Edge\TEMPORARY\Premium'
filepath_risk = r'\\svrtcs04\Syndicate Data\Actuarial\Pricing\2_Account_Pricing\NFS_Edge\TEMPORARY\Risk'

files_premium = glob.glob(filepath_premium + r'\*.xls*')
files_risk = glob.glob(filepath_risk + r'\*.xls*')

files_premium = [file for file in files_premium if '$' not in file]
files_risk = [file for file in files_risk if '$' not in file]


# %% risk files

risk_data = pd.DataFrame()

for file in files_risk:
    xls = xlrd.open_workbook(file, on_demand=True)

    sheets = [sheet for sheet in xls.sheet_names()]

    for sheet in sheets:
        df = pd.read_excel(file, sheet_name=sheet)
        risk_data = risk_data.append(df, ignore_index=True)


risk_data.to_excel(r'\\svrtcs04\Syndicate Data\Actuarial\Pricing\2_Account_Pricing\NFS_Edge\TEMPORARY\Risk\combined_bdx.xlsx')

# %% premium files

def combine_bdx(files):

    data = pd.DataFrame()

    for file in files:
        xls = xlrd.open_workbook(file, on_demand=True)

        sheets = [sheet for sheet in xls.sheet_names()]

        for sheet in sheets:
            df = pd.read_excel(file, sheet_name=sheet)
            data = data.append(df, ignore_index=True)
    
    return data


prem_bdx = combine_bdx(files_premium)

prem_bdx.to_excel(r'\\svrtcs04\Syndicate Data\Actuarial\Pricing\2_Account_Pricing\NFS_Edge\TEMPORARY\Premium\combined_bdx.xlsx')


# %% to SQL

sql_engine = sql_connection('tcspmSMDB02', 'PricingDevelopment')

prem_bdx.drop(labels=['Index'], axis=1, inplace=True)

prem_bdx.dropna(axis=0, how='any', subset=['Certificate Ref'], inplace=True)

prem_bdx_no_index = prem_bdx.drop(labels=['Index'], axis=1, inplace=False)

prem_bdx.to_sql('aon_edge_testing', sql_engine, schema='bdx', index=True, chunksize=1000, if_exists='replace')


# %%
