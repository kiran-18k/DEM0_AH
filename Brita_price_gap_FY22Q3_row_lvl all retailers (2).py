# -*- coding: utf-8 -*-
"""
Created on Thu Jan  6 11:08:26 2022

@author: Jishnu
"""
import pandas as pd
import numpy as np
from functools import reduce
import re

#Include Clorox Size Value in feed files. Did it manually in this refresh.

#%%
"""SECTION 1 :Reading all path """

#D:\Work\Pricing\Refresh\Price Gap Simulator\Refresh FY21Q1
pos_path = 'D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Refresh_all retailers/FY22Q3/POS/PS/'

upc_path = 'D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Refresh_all retailers/FY22Q3/POS/UPC/'

#Path to read CLorox BDA file
bda_path = 'D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Refresh_all retailers/FY22Q3/BDA Co-Efficient File/'

# Cust Agg file path
cust_path = 'D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Refresh_all retailers/FY22Q3/Custom Aggregates FY21Q1/'

#Path to reach competitor elasticity file
cda_path= 'D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Refresh_all retailers/FY22Q3/CDA/'

#Path to read mapping files
path = 'D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Refresh_all retailers/FY22Q3/'

qc='D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Refresh_all retailers/FY22Q3/QC/'

#%%
"""SECTION 2 : Reading all sheets of Mapping files
Load all sheets of Final Mapping file which has required PPG list, elasticity and 
other metric mapping for Price Gap Simulator"""

#Mapping file containing mapping for all products with POS,CDA,Cross elasticity
retailer_mapping = pd.ExcelFile(path+'Mapping Files/Final_Mapping_sheet_FY21Q3_all geo_latest.xlsx')
print(retailer_mapping.sheet_names)

# Load each sheet into respective DataFrames
retail_prod_map = retailer_mapping.parse('POS_mapping')
retail_prod_map =retail_prod_map [retail_prod_map ['Comment']=='Include']
comp_elas_map = retailer_mapping.parse('Competitor_Elasticites')
cross_elas_map = retailer_mapping.parse('Cross_Elasticites')
bda_map = retailer_mapping.parse('BDA')
bda_map_glad = retailer_mapping.parse('BDA_Glad')
upc_map = retailer_mapping.parse('BU_UPC_List')
ppl_map = retailer_mapping.parse('PPL_mapping')
contrib_factor = retailer_mapping.parse('Contrib_Factor')
cat_map = retailer_mapping.parse('cat_map')
gr_geo_map = retailer_mapping.parse('Grocery_mapping')

#%%
"""SECTION 3 : POS MANIPULATAION (PULL POS MEASURES FOR REQUIRED PPGs IN THE MAPPING FILE FOR OUR PRICE GAP SIMULATOR"""

#For all BU,list of columns to be kept after joining POS information to the 'POS Mapping' sheet of our mappping file(Final Mapping) 
f_col=['BU','Channel','Type','Products from Mapping file','Clorox Product', 'POS Products','Brand',
      'Sub Brand','size','Manufacturer','Geography','Clorox Sub Category Value','Clorox Segment Value','Standard Hierarchy Level',
      'SCBV','Stat Case Volume','Unit Sales','Dollar Sales','Volume Sales',
      'Baseline Dollars','Baseline Units','Baseline Volume','BPE']

#%%
#Aggregation func used to have the Category totals for walmart at a hierarchy level
"""please be aware this aggregation is not as per the sub-category in POS data file.
It is as per the category defination in CVM file, which can be segment,sub-category or combination both"""
def cat_agg(x):
    d = {}
    d['Cat_Tot SCBV'] = x['Stat Case Baseline Volume'].sum()
    d['Cat_Tot Stat Case Volume'] = x[ 'Stat Case Volume'].sum()   
    d['Cat_Tot Unit Sales'] = x['Unit Sales'].sum()
    d['Cat_Tot Dollar Sales'] = x['Dollar Sales'].sum()
    d['Cat_Tot Volume Sales'] = x['Volume Sales'].sum()
    d['Cat_Tot Baseline Dollars'] = x['Baseline Dollars'].sum()
    d['Cat_Tot Baseline Volume'] = x['Baseline Volume'].sum()
    d['Cat_Tot Baseline Units'] = x['Baseline Units'].sum()
    return pd.Series(d, index=['Cat_Tot SCBV','Cat_Tot Stat Case Volume','Cat_Tot Unit Sales',
                'Cat_Tot Dollar Sales','Cat_Tot Volume Sales','Cat_Tot Baseline Dollars',
                'Cat_Tot Baseline Units','Cat_Tot Baseline Volume'])

#Functions used to Aggregate POS measurs at segment level at size
def seg_agg(x):
    d = {}
    d['Seg_Tot SCBV'] = x['Stat Case Baseline Volume'].sum()
    d['Seg_Tot Stat Case Volume'] = x[ 'Stat Case Volume'].sum()   
    d['Seg_Tot Unit Sales'] = x['Unit Sales'].sum()
    d['Seg_Tot Dollar Sales'] = x['Dollar Sales'].sum()
    d['Seg_Tot Volume Sales'] = x['Volume Sales'].sum()
    d['Seg_Tot Baseline Dollars'] = x['Baseline Dollars'].sum()
    d['Seg_Tot Baseline Volume'] = x['Baseline Volume'].sum()
    d['Seg_Tot Baseline Units'] = x['Baseline Units'].sum()
    return pd.Series(d, index=['Seg_Tot SCBV','Seg_Tot Stat Case Volume','Seg_Tot Unit Sales',
                'Seg_Tot Dollar Sales','Seg_Tot Volume Sales','Seg_Tot Baseline Dollars',
                'Seg_Tot Baseline Units','Seg_Tot Baseline Volume'])

#%%

"""----------------------------START OF BDA MANIPULATION FOR ALL RETAILERS----------------------------------"""

"""1) BDA MANIPULATION ALL CHANNEL(Sub Brand Level, 4 cycles smoothened) """
    
#BDA manipulation for all BU's other than Glad
#load BDA data
# As number of retailers and time periods have increased, using separate source for each year since FY22Q1 
import os
print(os.listdir(bda_path))
os.chdir(bda_path)

import glob
file_list = glob.glob("*.xlsx") 
print(file_list)

bda_coeff_raw = pd.DataFrame()

#%%
for file in file_list:

    print(file)
    bda_coeff = pd.read_excel(bda_path + file)
    bda_coeff_raw = bda_coeff_raw.append(bda_coeff, ignore_index = True)

#%%
bda_raw_all = bda_coeff_raw[['model_source','Model_Period_End','catlib','Product_Level','Product_Name_Modeled','Product_Name_Current',
'Geography_Name','Geography_Level','Base_Price_Elasticity','Promo_Price_Elasticity','Base_Statcase_Volume','iriprod','prodkey']].drop_duplicates().reset_index(drop=True)
bda_raw_all = bda_raw_all.replace('NULL', np.nan, regex=True)
bda_raw_all['Product_Name_Modeled']=bda_raw_all['Product_Name_Modeled'].str.upper()
bda_raw_all.to_csv(qc+'bda_raw_all.csv')

#%%
# LA catlibs separated for automation. Check if new catlib available for LA
final_db1 = bda_raw_all[bda_raw_all['catlib'].isin(['BB','BF','B2','B4','B6','BS']) & bda_raw_all['Product_Level'].isin(['S','K','Z','I','X'])]
final_db1 = final_db1.drop_duplicates()
final_db1.to_csv(qc+'final_db1.csv')

#%%
#Mapping BDA to POS Retailers/Channels 
coeff_db_map = pd.read_excel(path+'Mapping Files/'+'Hyperion DB Channels.xlsx','Hyperion DB Channels')
dataf1 = final_db1.merge(coeff_db_map, on = ['Geography_Name', 'Geography_Level', 'model_source'], how = 'left')
dataf2 = dataf1[dataf1['IRI Channels'].isnull() == False]
dataf2.to_csv(qc+'dataf2.csv')

#%%
dataf2_w_iriprod = dataf2[dataf2['iriprod'].isnull()==False].reset_index(drop=True)
dataf2_wo_iriprod = dataf2[dataf2['iriprod'].isnull()==True].reset_index(drop=True) 

#%%
#Custom Aggregate Keys Mapping
df_Brita = pd.read_excel(cust_path+'CustAggs_FY22Q2 - Brita.xlsx', 'SKUs_to_Aggregate')
df_Brita = df_Brita[['Catcode','Prodlvl','Prodkey','Custprod','IRI_Product_Key','Product_Name']].drop_duplicates()
df_Brita.to_csv(qc+'df_Brita.csv')

df_cust = df_Brita.copy()

cust_agg_keys = df_cust[(df_cust['Prodlvl']=='S') & (pd.isnull(df_cust['Custprod'])==False)]
cust_agg_keys_w_cust_cnt = cust_agg_keys.groupby(['Custprod'])['Custprod'].count().reset_index(name="count")
cust_agg_keys_w_cust_cnt.to_csv(qc+'cust_agg_keys_w_cust_cnt.csv')

#%%
cust_agg_keys1 = cust_agg_keys.merge(cust_agg_keys_w_cust_cnt, on = ['Custprod'], how = 'left')
cust_agg_keys1.to_csv(qc+'cust_agg_keys1.csv')

#%%
dataf3_1 = dataf2_wo_iriprod.merge(cust_agg_keys1, left_on=['prodkey'], right_on=['Custprod'], how = 'left')
dataf3_1['Base_Statcase_Volume2'] = dataf3_1.apply(lambda x: x['Base_Statcase_Volume'] if pd.isnull(x['Custprod'])==True
                      else x['Base_Statcase_Volume']/x['count'], axis=1)
dataf3_1.drop(['iriprod'],axis=1,inplace=True)
dataf3_1.rename(columns={'IRI_Product_Key':'iriprod'},inplace=True)
dataf3_1.to_csv(qc+'dataf3_1.csv')

#%%
dataf3 = dataf3_1.append([dataf2_w_iriprod])
dataf3['Base_Statcase_Volume'] = dataf3.apply(lambda x: x['Base_Statcase_Volume'] if pd.isnull(x['count'])==True
                      else x['Base_Statcase_Volume2'], axis=1)
dataf3.to_csv(qc+'dataf3.csv')

dataf4 = dataf3[['model_source', 'Geography_Level', 'Geography_Name', 'IRI Channels', 'Model_Period_End',
    'Product_Level','catlib','Product_Name_Modeled','Product_Name_Current','Product_Name','prodkey',
    'CLOROX VS COMP','iriprod','Base_Price_Elasticity', 'Promo_Price_Elasticity','Base_Statcase_Volume']]
dataf5 = dataf4[dataf4['Base_Statcase_Volume']>0]
dataf5.to_csv(qc+'dataf5.csv')

#%%
def roll_a(x):
    d = {} 
    d['Base_Statcase_Volume'] = x['Base_Statcase_Volume'].sum()
    d['Promo_Price_Elasticity'] = np.average(x['Promo_Price_Elasticity'], weights=x['Base_Statcase_Volume'])
    d['Base_Price_Elasticity'] = np.average(x['Base_Price_Elasticity'], weights=x['Base_Statcase_Volume'])
    return pd.Series(d, index=['Promo_Price_Elasticity','Base_Price_Elasticity','Base_Statcase_Volume'])

#%%
CoefDb_All = dataf5.groupby(['iriprod','IRI Channels', 'CLOROX VS COMP', 'Model_Period_End', 'catlib']).apply(roll_a).reset_index()
CoefDb_All = CoefDb_All.rename(columns={'IRI Channels':'Geography'})
CoefDb_All.to_csv(qc+'CoefDb_All.csv')

#%%
#Start of manipulation to determine BDA lite for Brita. Check if there is any new catlib for Brita before proceeding.
Brita = CoefDb_All[CoefDb_All['catlib'].isin(['B2','B4','B6','BB','BF','BS'])]
Brita.to_csv(qc+'Brita.csv')

#%%
Brita_New = Brita[Brita['Model_Period_End']>='2021-03-28']
Brita_Old = Brita[Brita['Model_Period_End']<'2021-03-28']

#%%
Brita_pivot = pd.pivot_table(Brita_Old, values=['Base_Statcase_Volume'], index=['catlib', 'Model_Period_End','CLOROX VS COMP'],
                    columns =['Geography'], aggfunc = {'Base_Statcase_Volume' : sum})

#%%
Brita_pivot.columns = Brita_pivot.columns.droplevel(0)
Brita_1 = Brita_pivot.reset_index().rename_axis(None, axis=1)
Brita_1.to_csv(qc+'Brita_1.csv')

#%%
#Deleting catlib and model period for which Wal and TUS is absent - BDA Lite
Brita_1.dropna(subset=['Walmart Corp-RMA - Walmart','Total US - Food'],inplace=True)
Brita_2 = Brita_1.melt(['catlib','Model_Period_End','CLOROX VS COMP'], var_name ='Geography')
Brita_2.to_csv(qc+'Brita_2.csv')

#%%
Brita_2 = Brita_2[['catlib','Model_Period_End','CLOROX VS COMP','Geography']].drop_duplicates()
Brita_final = Brita_2.merge(Brita_Old, on=['catlib','Model_Period_End','CLOROX VS COMP','Geography'],how='left')
Brita_final.to_csv(qc+'Brita_final_check.csv')

#%%
# Dropping all such rows for which a catlib is not modelled for a particular retailer in a period. Came as as result of pivot.
Brita_final.dropna(subset=['Base_Statcase_Volume'],inplace=True)

#%%
Brita_final = Brita_final.append([Brita_New])

#%%
Brita_final['new'] = Brita_final['iriprod'].str.split(':')
Brita_final['new_subb'] = Brita_final['new'].apply(lambda x : x[0:5])
Brita_final['New iriprod subb'] = Brita_final['new_subb'].str.join(':')
Brita_final['new_mdl_size'] = Brita_final['new'].apply(lambda x : x[5:8])
Brita_final['New mdl size'] = Brita_final['new_mdl_size'].str.join(':')
Brita_final.rename(columns = {'iriprod':'old iriprod'},inplace=True)

#%%
brita_key_map = pd.read_excel(path+'Mapping Files/'+'Brita_Key_Mapping.xlsx', 'Brita')
Brita_final = Brita_final.merge(brita_key_map, left_on = ['New iriprod subb'], right_on = ['Subb IRI Key'], how ='left') 
Brita_final['iriprod'] = Brita_final['Subb Pdt Key'] + ":" + Brita_final['New mdl size']
Brita_final.drop(['new','new_subb','new_mdl_size'],axis=1,inplace=True)
Brita_final = Brita_final[Brita_final['iriprod'].notna()]
Brita_final.to_csv(qc+'Brita_iri_corr.csv')

#%%
#New Product Key for Brand, Subbrand Mapping
Brita_final['new'] = Brita_final['iriprod'].str.split(':')
Brita_final['new_split_irip'] = Brita_final['new'].apply(lambda x : x[:-3])
Brita_final['New iriprod'] = Brita_final['new_split_irip'].str.join(':')
Brita_final.drop(['new','new_split_irip'],axis=1,inplace=True)
Brita_final.to_csv(qc+'Brita_final.csv')

#End of manipulation to determine BDA lite for Brita

#%%
#Check if there is any new catlib for Brita before proceeding.
CoefDb_All = Brita_final.copy()

# Need to reset index so that Ranks can be assigned later (Avoid duplication of index for ranking)
CoefDb_All.reset_index(drop=True, inplace=True)
CoefDb_All.to_csv(qc+'CoefDb_All_check.csv')

#%%
# For Cured view - ranking based on geo and ret
#Select Latest 4 periods for all retailers and product keys
CoefDb_All['date'] = pd.to_datetime(CoefDb_All['Model_Period_End'],format='%Y-%m-%d')
CoefDb_All['year'] = pd.DatetimeIndex(CoefDb_All['date']).year
CoefDb_All['month'] = pd.DatetimeIndex(CoefDb_All['date']).month
CoefDb_All['Rank'] = CoefDb_All.sort_values(['Geography','iriprod','CLOROX VS COMP','year','month'], ascending = False).groupby(['Geography','iriprod', 'CLOROX VS COMP']).cumcount()+1
CoefDb_All.to_csv(qc+'CoefDb_All_ranked.csv')

#%%
CoefDb_All_Cl1 = CoefDb_All[CoefDb_All['Rank']<=4]
CoefDb_All_Cl1.to_csv(qc+'CoefDb_latest_4.csv')
CoefDb_All_Cl2 = CoefDb_All_Cl1.groupby(['iriprod','Geography']).apply(roll_a).reset_index()

#%%
#Proxy calculation for Retailers
#1 BJ's Corp-RMA - Club-> Proxy: Sam's Corp-RMA - Club
#2.1 Petco Corp-RMA - Pet -> Proxy: Target Corp-RMA - Mass, Walmart Corp-RMA - Walmart, Total US - Food
#2.2 Total Mass Aggregate -> Proxy: Target Corp-RMA - Mass, Walmart Corp-RMA - Walmart, Total US - Food
#2.3 Total US - Drug -> Proxy: Target Corp-RMA - Mass, Walmart Corp-RMA - Walmart, Total US - Food

#1
CoefDb_All_BJ = CoefDb_All_Cl2[CoefDb_All_Cl2['Geography'] == "Sam's Corp-RMA - Club"]
CoefDb_All_BJ['Geography'] = "BJ's Corp-RMA - Club"
CoefDb_All_TGWLMF = CoefDb_All_Cl2[CoefDb_All_Cl2['Geography'].isin(['Target Corp-RMA - Mass', 'Walmart Corp-RMA - Walmart', 'Total US - Food'])]
CoefDb_All_AGG = CoefDb_All_TGWLMF.groupby(['iriprod']).apply(roll_a).reset_index()

#2.1
CoefDb_All_PCo = CoefDb_All_AGG.copy(deep=True)
CoefDb_All_PCo['Geography'] = "Petco Corp-RMA - Pet"

#2.2
CoefDb_All_TMA = CoefDb_All_AGG.copy(deep=True)
CoefDb_All_TMA['Geography'] = "Total Mass Aggregate"

#2.3
CoefDb_All_TUG = CoefDb_All_AGG.copy(deep=True)
CoefDb_All_TUG['Geography'] = "Total US - Drug"

#%%
#3 Total US - Multi Outlet -> Proxy: Target Corp-RMA - Mass, Walmart Corp-RMA - Walmart, Total US - Food, Sam's Corp-RMA - Club
CoefDb_All_TGWLMFS = CoefDb_All_Cl2[CoefDb_All_Cl2['Geography'].isin(['Target Corp-RMA - Mass', 'Walmart Corp-RMA - Walmart', 'Total US - Food', "Sam's Corp-RMA - Club"])]
CoefDb_All_AGG2 = CoefDb_All_TGWLMFS.groupby(['iriprod']).apply(roll_a).reset_index()

#3
CoefDb_All_MULO = CoefDb_All_AGG2.copy(deep=True)
CoefDb_All_MULO['Geography'] = "Total US - Multi Outlet"

#%%
CoefDb_All_F = CoefDb_All_Cl2.append([CoefDb_All_BJ, CoefDb_All_PCo, CoefDb_All_TMA, CoefDb_All_TUG, CoefDb_All_MULO])
CoefDb_All_F.to_csv(qc+'CoefDb_All_F.csv')

#%%
dt_iri_raw = pd.read_excel(pos_path + 'Brita.xlsx', skiprows = 1)
dt_iri_raw = dt_iri_raw[dt_iri_raw['Standard Hierarchy Level'].isin(['SIZE_WATERFILTER_H1_6'])].reset_index(drop=True)
#dt_upc_raw = pd.read_excel(upc_path + 'MARKETING_PRICING_Brita UPC level_Q4FY21_for (m).xlsx', 'MARKETING_PRICING_Brita UPC lev', skiprows = 1)
#dt_iri_raw = dt_iri_raw.append(dt_upc_raw, ignore_index=True)

dt_iri_raw = dt_iri_raw.replace('AlbertsonsCo Corp-RMA - Food','ABSCO Corp-RMA - Food', regex=True)
dt_iri_raw ['Product'] = dt_iri_raw ['Product'].str.strip()
dt_iri_raw['SCBV_new'] = dt_iri_raw.apply(lambda x: x['Baseline Volume']/(x['Volume Sales']/x['Stat Case Volume']) if
(pd.isna(x['Stat Case Baseline Volume']) and (~pd.isna(x['Stat Case Volume']))) else x['Stat Case Baseline Volume'],axis=1)
dt_iri_raw.to_csv(qc+'dt_pos_all_Brita.csv')

dt_iri_raw.drop(columns=['Stat Case Baseline Volume'],axis=1,inplace=True)
dt_iri_raw.rename(columns={'SCBV_new':'Stat Case Baseline Volume'},inplace=True)

#BRITA POS manipulation
dt_brita = dt_iri_raw.copy()

dt_brita['size'] = dt_brita['Clorox Size Value'].str.extract('(\d*\.\d+|\d+)').astype(float)
dt_brita.to_csv(qc+'dt_brita.csv')


#%%
#Latest 4 Period Aggregated
#1. Left Join 
POS_CoefDb_All = dt_brita.merge(CoefDb_All_F, left_on=['Product Key','Geography'],right_on=['iriprod','Geography'], how='left')
POS_CoefDb_All.to_csv(qc+'POS_CoefDb_All.csv')

#%%
#Filtering out mapped POS+BDA after Key-Mapping
POS_CoefDb_All_mapped = POS_CoefDb_All.loc[POS_CoefDb_All['iriprod'].notnull()]

# 1st df to be appended
POS_CoefDb_All_mapped.to_csv(qc+'POS_CoefDb_All_mapped.csv')

#%%
#Filtering out unmapped POS+BDA after Key-Mapping
POS_CoefDb_All_unmapped = POS_CoefDb_All.loc[POS_CoefDb_All['iriprod'].isnull()]
POS_CoefDb_All_unmapped.to_csv(qc+'POS_CoefDb_All_unmapped.csv')

#%%
#New Product Key = Product Key - 2nd last key
iri_df3 = POS_CoefDb_All_unmapped.drop(['iriprod', 'Promo_Price_Elasticity','Base_Price_Elasticity', 'Base_Statcase_Volume'], axis = 1)
iri_df3['new'] = iri_df3['Product Key'].str.split(':')
iri_df3['new_split_pk'] = iri_df3['new'].apply(lambda x : [x[index] for index in [0,1,2,3,4,5,7]])
iri_df3['New Product Key'] = iri_df3['new_split_pk'].str.join(':')
iri_df3.drop(['new','new_split_pk'],axis=1,inplace=True)
iri_df3.to_csv(qc+"iri_df3.csv",index=False)

#%%
#New iriprod = iriprod - 2nd last key
CoefDb_All_F['new_split'] = CoefDb_All_F['iriprod'].str.split(':')
CoefDb_All_F['new_split_iri'] = CoefDb_All_F['new_split'].apply(lambda x : [x[index] for index in [0,1,2,3,4,5,7]])
CoefDb_All_F['New iriprod'] = CoefDb_All_F['new_split_iri'].str.join(':')
CoefDb_All_F.drop(['new_split','new_split_iri'],axis=1,inplace=True)
CoefDb_All_F.to_csv(qc+"CoefDb_All_F_new_iri_prod.csv",index=False)

#%%
def proxy_roll_a(x):
    d = {} 
    d['Base_Statcase_Volume'] = x['Base_Statcase_Volume'].mean()
    d['Promo_Price_Elasticity'] = np.average(x['Promo_Price_Elasticity'], weights=x['Base_Statcase_Volume'])
    d['Base_Price_Elasticity'] = np.average(x['Base_Price_Elasticity'], weights=x['Base_Statcase_Volume'])
    return pd.Series(d, index=['Promo_Price_Elasticity','Base_Price_Elasticity','Base_Statcase_Volume'])

#%%
#bda aggregation after Key - 2nd last key
CoefDb_All_F_Agg = CoefDb_All_F.groupby(['New iriprod','Geography']).apply(proxy_roll_a).reset_index()
CoefDb_All_F_Agg.to_csv(qc+"CoefDb_All_F_new_iri_prod1.csv",index=False)

#%%
#1. Left Join unmapped POS with BDA at Key - 2nd last key
POS_CoefDb_All_nw_pdt_key = iri_df3.merge(CoefDb_All_F_Agg, left_on=['New Product Key','Geography'], right_on=['New iriprod','Geography'], how='left')
POS_CoefDb_All_nw_pdt_key.to_csv(qc+"POS+Elasticity_nw_pdt_key.csv",index=False)

#%%
#Rule 1 completed - Appending Key - Mapped data with Key - 2nd last key mapped
POS_CoefDb_All_updated = POS_CoefDb_All_mapped.append(POS_CoefDb_All_nw_pdt_key)
POS_CoefDb_All_updated.to_csv(qc+'POS+Elasticity_RULE1.csv')

#%%
#Filtering out mapped POS+BDA after Key and Key-2nd last key Mapping
POS_CoefDb_All_updated_mapped = POS_CoefDb_All_updated.loc[POS_CoefDb_All_updated['iriprod'].notnull() | POS_CoefDb_All_updated['New iriprod'].notnull()] 
POS_CoefDb_All_updated_mapped.to_csv(qc+'POS_CoefDb_All_updated_mapped.csv')

#%%
#Filtering out unmapped POS+BDA after Key and Key-2nd last key Mapping
POS_CoefDb_All_updated_unmapped = POS_CoefDb_All_updated.loc[pd.isnull(POS_CoefDb_All_updated['iriprod']) & pd.isnull(POS_CoefDb_All_updated['New iriprod'])]
POS_CoefDb_All_updated_unmapped.to_csv(qc+'POS+Elasticity_updated_unmapped.csv')

#%%
#Filtering out unmapped POS+BDA after Key and Key-2nd last key Mapping having only Food and Mass Retailers
POS_CoefDb_All_unmapped_FOMA = POS_CoefDb_All_updated_unmapped[~POS_CoefDb_All_updated_unmapped['Geography'].isin(['Total US - Food', 
'Total US - Multi Outlet', 'Total Mass Aggregate', 'Total US - Drug', 'Petco Corp-RMA - Pet', "Sam's Corp-RMA - Club", "BJ's Corp-RMA - Club"])]

#Filtering out unmapped POS+BDA after Key and Key-2nd last key Mapping having all Retailers/Channels except Food and Mass
POS_CoefDb_All_unmapped_TCP = POS_CoefDb_All_updated_unmapped[POS_CoefDb_All_updated_unmapped['Geography'].isin(['Total US - Food', 
'Total US - Multi Outlet', 'Total Mass Aggregate', 'Total US - Drug', 'Petco Corp-RMA - Pet', "Sam's Corp-RMA - Club", "BJ's Corp-RMA - Club"])]

#%%
#POS data for unmapped after Key Mapping
iri_df4 = POS_CoefDb_All_unmapped_FOMA.drop(['iriprod','New iriprod','Promo_Price_Elasticity','Base_Price_Elasticity', 
                                'Base_Statcase_Volume'], axis = 1)
iri_df4.rename(columns = {'Geography':'Geography_unmapped'},inplace=True)
iri_df4.to_csv(qc+'iri_df4.csv')
Geography_unmapped = iri_df4['Geography_unmapped'].unique()
print(Geography_unmapped)

#%%
#Reading the geography proxy file. This file needs to be updated everytime there is a new unmapped Geography in iri_df4  
geo_pxy  = pd.read_csv(path +'Mapping Files/'+'Geo Proxy Mapping.csv', low_memory=False)

#%%
# Iterating through the list of unmapped retailers
POS_CoefDb_RULE2_0 = pd.DataFrame()
for geo in Geography_unmapped:
    print(geo)
    iri_df4_Geo = iri_df4[iri_df4['Geography_unmapped'] == geo ] 
    iri_df4_Geo =  iri_df4_Geo.merge(geo_pxy, on = ['Geography_unmapped'], how = 'inner')
    POS_CoefDb_RULE2_0 = POS_CoefDb_RULE2_0.append([iri_df4_Geo.merge(CoefDb_All_F_Agg, left_on = ['New Product Key','Geography_Proxy'], right_on = 
    ['New iriprod','Geography'], how = 'left')])

POS_CoefDb_RULE2_0['Geography Proxy'] = 'Yes'
POS_CoefDb_RULE2_0.to_csv(qc+'POS_CoefDb_RULE2_0.csv')

#%%
#Filtering out only the BDA file information from the appended data
CoefDb_RULE2 = POS_CoefDb_RULE2_0[['New iriprod','Geography_unmapped','Promo_Price_Elasticity','Base_Price_Elasticity', 
                                   'Base_Statcase_Volume']]

#Duplicates are formed in the BDA file as each retailer within a channel gets mapped to multiple retailers within a channel
#Duplicates removed and BDA rolled once again 
CoefDb_RULE2 = CoefDb_RULE2.drop_duplicates()
CoefDb_RULE2_rolled = CoefDb_RULE2.groupby(['New iriprod','Geography_unmapped']).apply(proxy_roll_a).reset_index() 

#%%
#Dropping Geo, Geo keys and BDA data from the appended dataframe
POS_CoefDb_RULE2_1 = POS_CoefDb_RULE2_0.drop(['Geography','Geography_Proxy','Promo_Price_Elasticity',
                                              'Base_Price_Elasticity', 'Base_Statcase_Volume'],axis=1)

#Each retailer does not get mapped to all retailers within a channel. Dropping all such rows.
POS_CoefDb_RULE2_1.dropna(subset = ["New iriprod"], inplace=True)

#Duplicates on POS data fromed due to same reason as above. Those being dropped.
POS_CoefDb_RULE2_1 = POS_CoefDb_RULE2_1.drop_duplicates()
            
#%%
#Left Join POS after duplicate removal with rolled up BDA. Completion of Rule 2
# 3rd df to be appended
POS_CoefDb_RULE2 = POS_CoefDb_RULE2_1.merge(CoefDb_RULE2_rolled, on=['New iriprod','Geography_unmapped'],how='left')
POS_CoefDb_RULE2.rename(columns = {'Geography_unmapped':'Geography'},inplace=True)

#%%
#Dropping Geo, Geo keys and BDA data from the appended dataframe
POS_CoefDb_RULE2_1_0 = POS_CoefDb_RULE2_0.drop(['Geography','Geography_Proxy','Promo_Price_Elasticity',
                                              'Base_Price_Elasticity', 'Base_Statcase_Volume'],axis=1)

#Each retailer does not get mapped to all retailers within a channel. Appending all such rows.
POS_CoefDb_RULE2_1_0 = POS_CoefDb_RULE2_1_0[POS_CoefDb_RULE2_1_0['New iriprod'].isna()].reset_index(drop=True)

#Duplicates on POS data fromed due to same reason as above. Those being dropped.
POS_CoefDb_RULE2_1_0 = POS_CoefDb_RULE2_1_0.drop_duplicates()
POS_CoefDb_RULE2_1_0.rename(columns = {'Geography_unmapped':'Geography'},inplace=True)

#%%
#Appending Unmapped Food data with Rule 2 data
POS_CoefDb_RULE2 = POS_CoefDb_RULE2.append([POS_CoefDb_RULE2_1_0])

#%%
POS_CoefDb_RULE2['is_duplicate'] = POS_CoefDb_RULE2[['Geography','Product Key','Product']].duplicated()
POS_CoefDb_RULE2_nd = POS_CoefDb_RULE2[POS_CoefDb_RULE2['is_duplicate']== False]
POS_CoefDb_RULE2_d = POS_CoefDb_RULE2[POS_CoefDb_RULE2['is_duplicate']== True] 
POS_CoefDb_RULE2_d = POS_CoefDb_RULE2_d[POS_CoefDb_RULE2_d['New iriprod'].notna()]
POS_CoefDb_RULE2 = POS_CoefDb_RULE2_nd.append([POS_CoefDb_RULE2_d])
POS_CoefDb_RULE2.to_csv(qc+'POS_CoefDb_RULE2.csv')

#%%
#Appending Unmapped Total, Club data with Rule 2 data
POS_CoefDb_RULE2_All = POS_CoefDb_All_unmapped_TCP.append([POS_CoefDb_RULE2])

#%%
#Appending mapped Key data with Rule 2 and Unmapped Total, Club data. Completion of Rule 1+2
POS_CoefDb_RULE12 = POS_CoefDb_All_updated_mapped.append([POS_CoefDb_RULE2_All])
POS_CoefDb_RULE12.to_csv(qc+'POS_CoefDb_RULE1+2.csv')

#%%
#extracting columns which are necessary
bda_catlib = bda_coeff_raw[['catlib','Model_Period_End','Product_Level','Geography_Name', 'Geography_Level', 
                            'model_source','iriprod','Product_Name_Current','Category_Name','prodkey'
                            ,'Base_Price_Elasticity','Promo_Price_Elasticity','Base_Statcase_Volume']]
bda_catlib = bda_catlib.replace('NULL', np.nan, regex=True)
bda_catlib = bda_catlib[bda_catlib['catlib'].isin(['B2','B4','B6','BB','BF','BS']) & bda_catlib['Product_Level'].isin(['S','X','K','Z','I'])]

#%%
#Mapping BDA to POS Retailers/Channels 
coeff_db_map = pd.read_excel(path+'Mapping Files/'+'Hyperion DB Channels.xlsx','Hyperion DB Channels')
bda_catlib = bda_catlib.merge(coeff_db_map, on = ['Geography_Name', 'Geography_Level', 'model_source'], how = 'left')
bda_catlib = bda_catlib[bda_catlib['IRI Channels'].isnull() == False]
bda_catlib.to_csv(qc+'bda_catlib.csv')

#%%
#Creating Brand column
bda_catlib['Brand']='BRITA'
category_bda = bda_catlib['Category_Name'].unique()
print(category_bda)

bda_catlib['Product_Name_Current']=bda_catlib['Product_Name_Current'].str.upper()
bda_catlib['Category_Name']=bda_catlib['Category_Name'].str.upper()

#%%
#Creating Sub Brand column
bda_catlib['Sub_brand'] = bda_catlib['Brand']+ " " +bda_catlib.apply(lambda x: 'FMF' 
                                                                   if  x['Category_Name']=='BRITA FM FILTER' 
          else 'FMS' if x['Category_Name']=='BRITA FM SYSTEM'
          else 'BF' if x['Category_Name']=='BRITA ON-THE-GO FILT'
          else 'BS' if x['Category_Name']=='BRITA ON-THE-GO BTTL'
          else 'PTF LEGACY'   if ('LEGACY'   in x['Product_Name_Current']) & (x['Category_Name']=='BRITA PT FILTER')
          else 'PTF STREAM'   if ('STREAM'   in x['Product_Name_Current']) & (x['Category_Name']=='BRITA PT FILTER')
          else 'PTF LONGLAST/ELITE' if ('LONGLAST' in x['Product_Name_Current']) & (x['Category_Name']=='BRITA PT FILTER') 
          else 'PTS LEGACY'   if ('LEGACY'   in x['Product_Name_Current']) & (x['Category_Name']=='BRITA PT SYSTEM')
          else 'PTS STREAM'   if ('STREAM'   in x['Product_Name_Current']) & (x['Category_Name']=='BRITA PT SYSTEM')
          else 'PTS LONGLAST/ELITE' if ('LONGLAST' in x['Product_Name_Current']) & (x['Category_Name']=='BRITA PT SYSTEM')
          else 'NA',axis=1)
bda_catlib.to_csv(qc+'bda_catlib1.csv')

#%%
bda_catlib_w_iriprod = bda_catlib[bda_catlib['iriprod'].isnull()==False].reset_index(drop=True)
bda_catlib_wo_iriprod = bda_catlib[bda_catlib['iriprod'].isnull()==True].reset_index(drop=True)

#%%
dataf3_1 = bda_catlib_wo_iriprod.merge(cust_agg_keys1, left_on=['prodkey'], right_on=['Custprod'], how = 'left')
dataf3_1['Base_Statcase_Volume2'] = dataf3_1.apply(lambda x: x['Base_Statcase_Volume'] if pd.isnull(x['Custprod'])==True
                      else x['Base_Statcase_Volume']/x['count'], axis=1)
dataf3_1.drop(['iriprod'],axis=1,inplace=True)
dataf3_1.rename(columns={'IRI_Product_Key':'iriprod'},inplace=True)
dataf3_1.to_csv(qc+'dataf3_1.csv')

#%%
dataf3 = dataf3_1.append([bda_catlib_w_iriprod])
dataf3['Base_Statcase_Volume'] = dataf3.apply(lambda x: x['Base_Statcase_Volume'] if pd.isnull(x['count'])==True
                      else x['Base_Statcase_Volume2'], axis=1)
dataf3.to_csv(qc+'dataf3.csv')

#%%
dataf4 = dataf3[['model_source', 'Geography_Level', 'Geography_Name', 'IRI Channels', 'Model_Period_End',
    'Product_Level','catlib','Product_Name_Current','Product_Name','prodkey','Category_Name','Brand',
    'Sub_brand','CLOROX VS COMP','iriprod','Base_Price_Elasticity', 'Promo_Price_Elasticity','Base_Statcase_Volume']]
dataf6 = dataf4[dataf4['Base_Statcase_Volume']>0]
dataf6.to_csv(qc+'dataf6.csv')

#%%
dataf6['new'] = dataf6['iriprod'].str.split(':')
dataf6['len'] = dataf6['new'].str.len()
dataf6 = dataf6[dataf6['len'] == dataf6['len'].max()]
dataf6['new_subb'] = dataf6['new'].apply(lambda x : x[0:5])
dataf6['New iriprod subb'] = dataf6['new_subb'].str.join(':')
dataf6['new_mdl_size'] = dataf6['new'].apply(lambda x : x[5:8])
dataf6['New mdl size'] = dataf6['new_mdl_size'].str.join(':')
dataf6.rename(columns = {'iriprod':'old iriprod'},inplace=True)

#%%
brita_key_map = pd.read_excel(path+'Mapping Files/'+'Brita_Key_Mapping.xlsx', 'Brita')
bda_catlib_final = dataf6.merge(brita_key_map, left_on = ['New iriprod subb'], right_on = ['Subb IRI Key'], how ='left') 
bda_catlib_final['iriprod'] = bda_catlib_final['Subb Pdt Key'] + ":" + bda_catlib_final['New mdl size']
bda_catlib_final.drop(['new','new_subb','new_mdl_size'],axis=1,inplace=True)
bda_catlib_final.to_csv(qc+'bda_catlib_iri_corr.csv')

#%%
bda_catlib_final = bda_catlib_final[['Category_Name','Brand','Sub_brand','IRI Channels','iriprod','Model_Period_End',
    'Product_Name_Current']].drop_duplicates()

#%%
bda_catlib = CoefDb_All_Cl1.merge(bda_catlib_final, left_on = ['Geography','iriprod','Model_Period_End'], right_on = ['IRI Channels','iriprod','Model_Period_End'])
bda_catlib.to_csv(qc+'bda_catlib2.csv')

#%%
# Filter Cat name = Key Acc RMAs for Sub_brand defn 
bda_catlib_rma = bda_catlib[bda_catlib['Category_Name']=='KEY ACCOUNT RMAS'] 
 #Creating Sub Brand column
bda_catlib_rma['Sub_brand'] = bda_catlib_rma['Brand']+ " " + bda_catlib_rma.apply(lambda x: 'FMF' 
                                                                   if  x['Subb Product']=='BRITA FAUCET MOUNT FILTERS (FMF)' 
          else 'FMS' if x['Subb Product']=='BRITA FAUCET MOUNT FILTRATION SYSTEMS (FMS)'
          else 'BF' if x['Subb Product']=='BRITA FILTERING BOTTLE FILTERS (FBF)'
          else 'BS' if x['Subb Product']=='BRITA FILTERING BOTTLE FILTRATION SYSTEMS (FBS)'
          else 'PTF LEGACY'   if x['Subb Product']=='BRITA LEGACY POUR THROUGH FILTERS (PTF)'
          else 'PTF STREAM'   if x['Subb Product']=='BRITA STREAM POUR THROUGH FILTERS (PTF)'
          else 'PTF LONGLAST/ELITE' if x['Subb Product']=='BRITA LONGLAST/ELITE POUR THROUGH FILTERS (PTF)'
          else 'PTS LEGACY'   if x['Subb Product']=='BRITA LEGACY POUR THROUGH FILTRATION SYSTEMS (PTS)'
          else 'PTS STREAM'   if x['Subb Product']=='BRITA STREAM POUR THROUGH FILTRATION SYSTEMS (PTS)'
          else 'PTS LONGLAST/ELITE' if x['Category_Name']=='BRITA LONGLAST/ELITE POUR THROUGH FILTRATION SYSTEMS (PTS)'
          else 'NA',axis=1)
bda_catlib_rma.to_csv(qc+'bda_catlib_rma.csv')

#%%
bda_catlib = bda_catlib[~bda_catlib['Category_Name'].isin(['KEY ACCOUNT RMAS'])] 
bda_catlib = bda_catlib.append(bda_catlib_rma,ignore_index=True)
bda_catlib.to_csv(qc+'bda_catlib3.csv')

#%%
#--------Filters
filters = bda_catlib[bda_catlib['Sub_brand'].isin(['BRITA FMF','BRITA BF','BRITA PTF STREAM',
                                                   'BRITA PTF LONGLAST/ELITE','BRITA PTF LEGACY','BRITA NA'])]

#%%%
def size(x,y):
    #Size extracted from Product Name Current in BDA file
    size= re.findall(r'[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?',x)
    res = list(size)
    return res[-1]+" "+ "CT" 

#%%%
filters['size'] = filters.apply(lambda x: size(x['Product_Name_Current'],x['Sub_brand']),axis=1)
filters.to_csv(qc+'filters.csv')

#%%
#Remove na subrands
filters_1 = filters[filters['Sub_brand']!='BRITA NA']
filters_1.to_csv(qc+'filters_1.csv')

#%%
# Roll bda measures for Filters Subbrand
def elas_agg(x):
        d = {}
        d['Base_Price_Elasticity'] = np.average(x['Base_Price_Elasticity'], weights = x['Base_Statcase_Volume'])
        d['Promo_Price_Elasticity'] = np.average(x['Promo_Price_Elasticity'], weights = x['Base_Statcase_Volume'])
        d['Base_Statcase_Volume']=x['Base_Statcase_Volume'].mean()
        return pd.Series(d, index=['Base_Price_Elasticity','Promo_Price_Elasticity','Base_Statcase_Volume'])

#%%%
bda_filters = filters_1.groupby(['Geography','Sub_brand','size']).apply(elas_agg).reset_index()
bda_filters.to_csv(qc+'bda_filters.csv')

#%%%
#--System---
system = bda_catlib[bda_catlib['Sub_brand'].isin(['BRITA FMS','BRITA BS','BRITA PTS STREAM',
                                                  'BRITA PTS LONGLAST/ELITE','BRITA PTS LEGACY','BRITA NA'])]
system_1 = system[system['Sub_brand']!='BRITA NA']
system_1.to_csv(qc+'system_1.csv')

#%%%
def sys_size(x,y):
    #Size extracted from Product Name Current in BDA file
    size= re.findall(r'[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?',x)
    res = list(size)
    print(list(size))
    return res[0]+" "+ "CUPS" 

#%%%
system_1['size'] = system_1.apply(lambda x: sys_size(x['Product_Name_Current'],x['Sub_brand']),axis=1)

# Why 1? To join with level_0_w_bda 
system_1['size'] = system_1.apply(lambda x: "1" if (x['Sub_brand'] in (['BRITA FMS','BRITA BS'])) 
                                      else x['size'],axis=1)
system_1.to_csv(qc+'system_1.csv')

#%%
# Roll bda measures for System Subbrand
bda_system = system_1.groupby(['Geography','Sub_brand','size']).apply(elas_agg).reset_index()
bda_system.to_csv(qc+'bda_system.csv')

#Combine bda filter and system data
bda_final = bda_filters.append(bda_system,ignore_index = True)
bda_final.to_csv(qc+'bda_final.csv')

#%%
#Filtering out mapped POS+BDA after 'GEO-KEY MAP', 'GEO-SIZE MAP', 'GEO PROXY-SIZE MAP'
POS_CoefDb_RULE12_mapped = POS_CoefDb_RULE12.loc[POS_CoefDb_RULE12['Base_Price_Elasticity'].notnull()] 
POS_CoefDb_RULE12_mapped.to_csv(qc+'POS_CoefDb_RULE12_mapped.csv')

#%%
#Filtering out unmapped POS+BDA after 'GEO-KEY MAP', 'GEO-SIZE MAP', 'GEO PROXY-SIZE MAP'
POS_CoefDb_RULE12_unmapped = POS_CoefDb_RULE12.loc[POS_CoefDb_RULE12['Base_Price_Elasticity'].isnull()] 
POS_CoefDb_RULE12_unmapped.to_csv(qc+'POS_CoefDb_RULE12_unmapped.csv')

#%%
#POS data for unmapped after 'GEO-KEY MAP', 'GEO-SIZE MAP', 'GEO PROXY-SIZE MAP'
iri_df5 = POS_CoefDb_RULE12_unmapped.drop(['iriprod','New iriprod','Promo_Price_Elasticity','Base_Price_Elasticity', 
                                'Base_Statcase_Volume'], axis = 1)
# Segment level
SubCat_remap = {'POUR THROUGH':'PT', 'FAUCET MOUNT':'FM', 'FILTERING BOTTLE':'B'}
Segment_remap = {'FILTERS (PTF)' :'F','FILTRATION SYSTEMS (PTS)':'S','FILTERS (FMF)':'F',
                'FILTRATION SYSTEMS (FMS)':'S', 'FILTERS (FBF)':'F', 'FILTRATION SYSTEMS (FBS)':'S'}
iri_df5['Segment_level'] = iri_df5['Clorox Brand Value'] +" "+iri_df5['Clorox Sub Category Value'].replace(SubCat_remap) + iri_df5['Clorox Segment Value'].replace(Segment_remap)
print(iri_df5['Segment_level'].unique())

# SubBrand level
iri_df5['SubBrand_level'] = iri_df5['Segment_level'] + " " + iri_df5.apply(lambda x: x['Clorox SubBrand Value'].replace(x['Clorox Brand Value'],"").strip(),axis=1)
iri_df5['SubBrand_level'] = iri_df5.apply(lambda x: x['SubBrand_level'].strip(),axis=1)

# create dummy size column
# Why 1? To join with level_0_w_bda 
iri_df5['dummy_size'] = iri_df5.apply(lambda x: "1" if (x['Segment_level'] in (['BRITA BS','BRITA FMS'])) 
                                      else x['Clorox Size Value'],axis=1)

# There will be duplications at 'Geography','SubBrand_level','dummy_size' as different products 
iri_df5.to_csv(qc+'iri_df5.csv')

#%%       
# Merge POS and new BDA data
# iri_df5 + bda_final is joined on Geo+Subb+Size
# There will be duplications at 'Geography','SubBrand_level','dummy_size' as different products
POS_CoefDb_GSS = pd.merge(iri_df5, bda_final, left_on =['Geography','SubBrand_level','dummy_size'],
                                  right_on=['Geography','Sub_brand','size'],how = 'left' )
POS_CoefDb_GSS.rename(columns ={'size_x':'size'},inplace=True)
POS_CoefDb_GSS.to_csv(qc+'POS_CoefDb_GSS.csv')

#%% 
POS_CoefDb_RULE12 = POS_CoefDb_RULE12_mapped.append([POS_CoefDb_GSS])
POS_CoefDb_RULE12.to_csv(qc+'POS_CoefDb_RULE1+2_new.csv')

#%%
#Filtering out mapped POS+BDA after Key, Key-2nd last key and Subb Mapping
POS_CoefDb_RULE12_mapped = POS_CoefDb_RULE12.loc[POS_CoefDb_RULE12['Base_Price_Elasticity'].notnull()] 
POS_CoefDb_RULE12_mapped.to_csv(qc+'POS_CoefDb_RULE12_mapped.csv')

#%%
#Filtering out unmapped POS+BDA after Key, Key-2nd last key and Subb Mapping
POS_CoefDb_RULE12_unmapped = POS_CoefDb_RULE12.loc[pd.isnull(POS_CoefDb_RULE12['Base_Price_Elasticity'])]
POS_CoefDb_RULE12_unmapped.to_csv(qc+'POS_CoefDb_RULE12_unmapped.csv')

#%%
#Filtering out unmapped POS+BDA after Key, Key-2nd last key and Subb Mapping having only Food and Mass Retailers
POS_CoefDb_RULE12_unmapped_FOMA = POS_CoefDb_RULE12_unmapped[~POS_CoefDb_RULE12_unmapped['Geography'].isin(['Total US - Food', 
'Total US - Multi Outlet', 'Total Mass Aggregate', 'Total US - Drug', 'Petco Corp-RMA - Pet', "Sam's Corp-RMA - Club", "BJ's Corp-RMA - Club"])]

#Filtering out unmapped POS+BDA after Key, Key-2nd last key and Subb Mapping having all Retailers/Channels except Food and Mass
POS_CoefDb_RULE12_unmapped_TCP = POS_CoefDb_RULE12_unmapped[POS_CoefDb_RULE12_unmapped['Geography'].isin(['Total US - Food', 
'Total US - Multi Outlet', 'Total Mass Aggregate', 'Total US - Drug', 'Petco Corp-RMA - Pet', "Sam's Corp-RMA - Club", "BJ's Corp-RMA - Club"])]

#%%
#POS data for unmapped after Key, Key-2nd last key and Subb Mapping
iri_df6 = POS_CoefDb_RULE12_unmapped_FOMA.drop(['iriprod','New iriprod','Promo_Price_Elasticity','Base_Price_Elasticity', 
                                'Base_Statcase_Volume','Sub_brand','size_y'], axis = 1)
iri_df6.rename(columns = {'Geography':'Geography_unmapped'},inplace=True)
iri_df6.to_csv(qc+'iri_df6.csv')
Geography_unmapped = iri_df6['Geography_unmapped'].unique()
print(Geography_unmapped)

#%%
#Reading the geography proxy file. This file needs to be updated everytime there is a new unmapped Geography in iri_df6  
geo_pxy  = pd.read_csv(path +'Mapping Files/'+'Geo Proxy Mapping.csv', low_memory=False)

#%%
# Iterating through the list of unmapped retailers
POS_CoefDb_RULE12_0 = pd.DataFrame()
for geo in Geography_unmapped:
    print(geo)
    iri_df6_Geo = iri_df6[iri_df6['Geography_unmapped'] == geo ] 
    iri_df6_Geo =  iri_df6_Geo.merge(geo_pxy, on = ['Geography_unmapped'], how = 'inner')
    POS_CoefDb_RULE12_0 = POS_CoefDb_RULE12_0.append([iri_df6_Geo.merge(bda_final, left_on =['Geography_Proxy','SubBrand_level','dummy_size'],
    right_on = ['Geography','Sub_brand','size'], how='left')])

POS_CoefDb_RULE12_0['Geography Proxy 2.1'] = 'Yes'
POS_CoefDb_RULE12_0.to_csv(qc+'POS_CoefDb_RULE12_0.csv')

#%%
#Filtering out only the BDA file information from the appended data
CoefDb_RULE12 = POS_CoefDb_RULE12_0[['Geography_unmapped','Sub_brand','size_y','Promo_Price_Elasticity','Base_Price_Elasticity', 
                                   'Base_Statcase_Volume']]

#Duplicates are formed in the BDA file as each retailer within a channel gets mapped to multiple retailers within a channel
#Duplicates removed and BDA rolled once again 
CoefDb_RULE12 = CoefDb_RULE12.drop_duplicates()
CoefDb_RULE12_rolled = CoefDb_RULE12.groupby(['Geography_unmapped','Sub_brand','size_y']).apply(elas_agg).reset_index() 

#%%
#Dropping Geo, Geo keys and BDA data from the appended dataframe
POS_CoefDb_RULE2_10 = POS_CoefDb_RULE12_0.drop(['Geography','Geography_Proxy','Promo_Price_Elasticity',
                    'Base_Price_Elasticity','Base_Statcase_Volume'],axis=1)

#Each retailer does not get mapped to all retailers within a channel. Dropping all such rows.
POS_CoefDb_RULE2_10.dropna(subset = ['Sub_brand','size_y'], inplace=True)

#Duplicates on POS data fromed due to same reason as above. Those being dropped.
POS_CoefDb_RULE2_10 = POS_CoefDb_RULE2_10.drop_duplicates()
            
#%%
#Left Join POS after duplicate removal with rolled up BDA. Completion of Geo Subb size Mapping
# 3rd df to be appended
POS_CoefDb_RULE121 = POS_CoefDb_RULE2_10.merge(CoefDb_RULE12_rolled, on=['Geography_unmapped','Sub_brand','size_y'],how='left')
POS_CoefDb_RULE121.rename(columns = {'Geography_unmapped':'Geography'},inplace=True)

#%%
#Dropping Geo, Geo keys and BDA data from the appended dataframe
POS_CoefDb_RULE12_1_0 = POS_CoefDb_RULE12_0.drop(['Geography','Geography_Proxy','Promo_Price_Elasticity',
                                              'Base_Price_Elasticity', 'Base_Statcase_Volume'],axis=1)

#Each retailer does not get mapped to all retailers within a channel. Appending all such rows.
POS_CoefDb_RULE12_1_0 = POS_CoefDb_RULE12_1_0[POS_CoefDb_RULE12_1_0['Sub_brand'].isna()].reset_index(drop=True)

#Duplicates on POS data fromed due to same reason as above. Those being dropped.
POS_CoefDb_RULE12_1_0 = POS_CoefDb_RULE12_1_0.drop_duplicates()
POS_CoefDb_RULE12_1_0.rename(columns = {'Geography_unmapped':'Geography'},inplace=True)

#%%
#Appending Unmapped Food data with Rule 2.1 data
POS_CoefDb_RULE12 = POS_CoefDb_RULE121.append([POS_CoefDb_RULE12_1_0])
POS_CoefDb_RULE12.to_csv(qc+'POS_CoefDb_RULE12.csv')

#%%
POS_CoefDb_RULE12['is_duplicate'] = POS_CoefDb_RULE12[['Geography','Product Key','Product']].duplicated()
POS_CoefDb_RULE12_nd = POS_CoefDb_RULE12[POS_CoefDb_RULE12['is_duplicate']== False]
POS_CoefDb_RULE12_d = POS_CoefDb_RULE12[POS_CoefDb_RULE12['is_duplicate']== True] 
POS_CoefDb_RULE12_d = POS_CoefDb_RULE12_d[POS_CoefDb_RULE12_d['Sub_brand'].notna()]
POS_CoefDb_RULE12 = POS_CoefDb_RULE12_nd.append([POS_CoefDb_RULE12_d])
POS_CoefDb_RULE12.rename(columns ={'size_x':'size'},inplace=True)
POS_CoefDb_RULE12.to_csv(qc+'POS_CoefDb_RULE12_GeoSubb.csv')

#%%
#Appending Unmapped Total, Club data with Rule 2.1 data
POS_CoefDb_RULE12_All = POS_CoefDb_RULE12_unmapped_TCP.append([POS_CoefDb_RULE12])

#%%
#Appending mapped Key data with Rule 2.1 and Unmapped Total, Club data. Completion of Rule 1+2+2.1
POS_CoefDb_RULE12 = POS_CoefDb_RULE12_mapped.append([POS_CoefDb_RULE12_All])
POS_CoefDb_RULE12.to_csv(qc+'POS_CoefDb_RULE1+2.csv')

#%%
#Creating MAP STAT and MAP TYPE columns
POS_CoefDb_RULE12['MAP STAT'] = np.where(POS_CoefDb_RULE12['Base_Price_Elasticity'].isnull(), 'UNMAP', 'MAP')

conditions = [(POS_CoefDb_RULE12['Base_Price_Elasticity'].isnull()),

(~POS_CoefDb_RULE12['Base_Price_Elasticity'].isnull() & 
~POS_CoefDb_RULE12['iriprod'].isnull() &
POS_CoefDb_RULE12['Geography Proxy'].isnull()),

(~POS_CoefDb_RULE12['Base_Price_Elasticity'].isnull() & 
~POS_CoefDb_RULE12['New iriprod'].isnull() &
POS_CoefDb_RULE12['Geography Proxy'].isnull()),

(~POS_CoefDb_RULE12['Base_Price_Elasticity'].isnull() & 
~POS_CoefDb_RULE12['New iriprod'].isnull() &
~POS_CoefDb_RULE12['Geography Proxy'].isnull()),

(~POS_CoefDb_RULE12['Base_Price_Elasticity'].isnull() & 
POS_CoefDb_RULE12['New iriprod'].isnull() &
~POS_CoefDb_RULE12['Sub_brand'].isnull() &
POS_CoefDb_RULE12['Geography Proxy 2.1'].isnull()), 

(~POS_CoefDb_RULE12['Base_Price_Elasticity'].isnull() & 
POS_CoefDb_RULE12['New iriprod'].isnull() &
~POS_CoefDb_RULE12['Sub_brand'].isnull() &
~POS_CoefDb_RULE12['Geography Proxy 2.1'].isnull())]

choices = ['UNMAP', 'GEO-KEY MAP', 'GEO-SIZE MAP', 'GEO PROXY-SIZE MAP', 'GEO-SUB-SIZE MAP', 'GEO PROXY-SUB-SIZE MAP']

POS_CoefDb_RULE12['MAP TYPE'] = np.select(conditions, choices, default=np.nan)
POS_CoefDb_RULE12.to_csv(qc+'POS_CoefDb_Brita.csv')

#%%
#Aggregate POS measures
def agg(x):
    d = {}
    d['SCBV'] = x['Stat Case Baseline Volume'].sum()
    d['Stat Case Volume'] = x[ 'Stat Case Volume'].sum()   
    d['Unit Sales'] = x['Unit Sales'].sum()
    d['Dollar Sales'] = x['Dollar Sales'].sum()
    d['Volume Sales'] = x['Volume Sales'].sum()
    d['Baseline Dollars'] = x['Baseline Dollars'].sum()
    d['Baseline Volume'] = x['Baseline Volume'].sum()
    d['Baseline Units'] = x['Baseline Units'].sum()
    d['BPE'] = np.average(x['Base_Price_Elasticity'], weights=x['Base_Statcase_Volume'])
    return pd.Series(d, index=['SCBV', 'Stat Case Volume','Unit Sales','Dollar Sales','Volume Sales','Baseline Dollars',
                            'Baseline Units','Baseline Volume','BPE'])

#%%
dt_brita_1 = POS_CoefDb_RULE12.groupby(['Geography','Product','size','Clorox Sub Category Value','Clorox Segment Value',
                                     'Standard Hierarchy Level']).apply(agg).reset_index()
dt_brita_1['Geography'] = dt_brita_1['Geography'].str.upper().str.strip() 
dt_brita_1.to_csv(qc+'dt_brita_1.csv')

#%%
#Filter only SKUs in our PPG list for Brita
retail_brita = pd.merge(retail_prod_map, dt_brita_1, left_on=['Channel','POS Products'],
              right_on=['Geography','Product'],how='left')
retail_brita = retail_brita[retail_brita['BU']=='BRITA']
brita = retail_brita[f_col] #keep required columns as specified above

# Imp QC - check for mapping
brita.to_csv(qc+'brita.csv')

#%%
#Aggregate POS measures for multiple Branded and PL
def pos_agg(x):
    d = {}
    d['SCBV'] = x['SCBV'].sum()
    d['Stat Case Volume'] = x[ 'Stat Case Volume'].sum()   
    d['Unit Sales'] = x['Unit Sales'].sum()
    d['Dollar Sales'] = x['Dollar Sales'].sum()
    d['Volume Sales'] = x['Volume Sales'].sum()
    d['Baseline Dollars'] = x['Baseline Dollars'].sum()
    d['Baseline Volume'] = x['Baseline Volume'].sum()
    d['Baseline Units'] = x['Baseline Units'].sum()
    return pd.Series(d, index=['SCBV', 'Stat Case Volume','Unit Sales','Dollar Sales','Volume Sales','Baseline Dollars',
                            'Baseline Units','Baseline Volume'])

#%%
# Manipulation to make single rows where multiple Branded/PL products are present
brita_1 = brita[brita['Type'].isin(['BRANDED PRODUCTS','PRIVATE LABEL'])]
brita_1 = brita_1.groupby(['BU','Channel','Type','Products from Mapping file','Clorox Product','Brand','Sub Brand',
'Manufacturer','size','Geography','Clorox Sub Category Value','Clorox Segment Value', 'Standard Hierarchy Level']).apply(pos_agg).reset_index()

#%%
brita_2 = brita[brita['Type'].isin(['CLOROX PRODUCTS'])]
brita = brita_2.append([brita_1],ignore_index=True)

# Imp QC - check for mapping
brita.to_csv(qc+'brita1.csv')
brita.drop(['POS Products'],axis=1,inplace=True)

#Category Aggregation as per CVM defination,and for channel walmart and size hierarchy
cat_raw = dt_iri_raw.copy()
cat_bri = cat_raw[cat_raw['Standard Hierarchy Level'].isin(['SIZE_WATERFILTER_H1_6'])]
cat_bri1 = cat_bri.groupby(['Geography','Clorox Segment Value']).apply(cat_agg).reset_index().rename(columns={'Clorox Segment Value':'Category Name'}) 
cat_bri1.to_csv(qc+'cat_bri1.csv')

#Segment level aggregation,and for channel walmart and size hierarchy
seg_raw = dt_iri_raw.copy()
seg_bri = seg_raw[seg_raw['Standard Hierarchy Level'].isin(['SIZE_WATERFILTER_H1_6'])]
seg_bri1 = seg_bri.groupby(['Geography','Clorox Segment Value']).apply(seg_agg).reset_index()
seg_bri1.to_csv(qc+'seg_bri1.csv')

#%%
cat_levels = [cat_bri1]
cat_total = reduce(lambda left,right: left.append(right, ignore_index = True), cat_levels)
cat_total.to_csv(qc+'cat_total.csv')  

seg_levels=[seg_bri1]
seg_total= reduce(lambda left,right: left.append(right, ignore_index = True), seg_levels)
seg_total.to_csv(qc+'seg_total.csv') 

#%%
#CDA Manipulation
cda_raw = pd.read_excel(cda_path+'CVM CY21 with Price_category - FY22Q2.xlsx','CY21 CVM output')
cda_1 = pd.merge(comp_elas_map, cda_raw[['Product','elasticity']],
                                  left_on='Product_Name_Modeled',right_on='Product',how='left')
cda_1 = cda_1.drop_duplicates()
cda_1.to_csv(qc+'cda_1.csv')    

#%%
#Load cross elasticty file
cross_data = pd.read_excel(cda_path+'Combined file for CDA.xlsx','All')
cross_raw = cross_data[['BU','Quarter','Prodlvl','ProductDescription','complvl','CompProductDescription',
           'Cross_BP_Coeff','Cross_BP_Elast','CrossBSCV']]

#remove duplicates
cross_raw['BU'] = cross_raw['BU'].str.upper()
cross_raw["is_duplicate"]= cross_raw.duplicated()
cross_raw.to_csv(qc+'cross_raw.csv')
cross_raw = cross_raw[cross_raw['is_duplicate']==False]

cross_raw_1 = cross_raw.drop(['is_duplicate'], axis=1)
cross_raw_1['Prod_Comp'] = cross_raw_1['ProductDescription']+cross_raw_1['CompProductDescription']
cross_raw_1['Prod_Comp'] = cross_raw_1['Prod_Comp'].str.strip()
cross_raw_1['Prod_Comp'] = cross_raw_1['Prod_Comp'].str.replace(' ','')
cross_raw_1['Prod_Comp'] = cross_raw_1['Prod_Comp'].str.upper()

#Cross Elasticity mapping file
cross_bu = cross_elas_map.copy()

#Manipulation for competitor 1
cross_bu['comp1_key']=cross_bu['Cross Elasticity Mapping']+ " " + cross_bu['Comp1']
cross_bu['comp1_key']=cross_bu['comp1_key'].str.strip()
cross_bu['comp1_key']=cross_bu['comp1_key'].str.replace(' ','')
cross_bu['comp1_key']=cross_bu['comp1_key'].str.upper()
comp_1 = pd.merge(cross_bu,cross_raw_1,left_on='comp1_key',right_on='Prod_Comp',how='left')
comp_1.to_csv(qc+'comp_1.csv')

#%%
#aggregate cross elasticities from different periods for each product
def cross_agg1(x):
    d = {}
    d['Cross_BP_Coeff_1'] = np.average(x['Cross_BP_Coeff'], weights=x['CrossBSCV'])
    return pd.Series(d, index=['Cross_BP_Coeff_1'])

comp_1_details = comp_1.groupby(['BU_x','Channel','Type','Products from Mapping file','Clorox Product',
                                 'Cross Elasticity Mapping', 'Comp1']).apply(cross_agg1).reset_index()
comp_1_details.to_csv(qc+'comp_1_details.csv')

#%%
#Assign values for products which do not have elasticities in the cross elasticity file
comp_1_details['Cross_BP_Coeff_1']=comp_1_details.apply(lambda x : 0.25 if (pd.isna(x['Cross_BP_Coeff_1']) & (x['Comp1']!='Not Applicable'))
                                   else x['Cross_BP_Coeff_1'], axis=1)
comp_1_details.rename(columns = {'BU_x':'BU'},inplace=True)
comp_1_details.to_csv(qc+'comp_1_final.csv')                                  

#%%
##Manipulation for competitor 2
cross_bu['comp2_key']=cross_bu['Cross Elasticity Mapping']+ " " + cross_bu['Comp2']
cross_bu['comp2_key']=cross_bu['comp2_key'].str.strip()
cross_bu['comp2_key']=cross_bu['comp2_key'].str.replace(' ','')
cross_bu['comp2_key']=cross_bu['comp2_key'].str.upper()

comp_2 = pd.merge(cross_bu,cross_raw_1,left_on='comp2_key',right_on='Prod_Comp',how='left')
comp_2.to_csv(qc+'comp_2.csv')

#%%
#aggregate cross elasticities from different periods for each product
def cross_agg(x):
    d = {}
    d['Cross_BP_Coeff_2'] = np.average(x['Cross_BP_Coeff'], weights=x['CrossBSCV'])
    return pd.Series(d, index=['Cross_BP_Coeff_2'])

comp_2_details = comp_2.groupby(['BU_x','Channel','Type','Products from Mapping file','Clorox Product',
                          'Cross Elasticity Mapping','Comp2']).apply(cross_agg).reset_index()
comp_2_details.to_csv(qc+'comp_2_details.csv')   

#%%
#Assign values for products which do not have elasticities in the cross elasticity file
comp_2_details['Cross_BP_Coeff_2'] = comp_2_details.apply(lambda x : 0.25 if (pd.isna(x['Cross_BP_Coeff_2']) & (x['Comp2']!='Not Applicable'))
                 else x['Cross_BP_Coeff_2'], axis=1)
comp_2_details.rename(columns = {'BU_x':'BU'},inplace=True)
comp_2_details.to_csv(qc+'comp_2_final.csv')

#%%
#Total competitor
comp_total = pd.merge(comp_1_details, comp_2_details,on=['BU','Channel','Type','Products from Mapping file','Clorox Product','Cross Elasticity Mapping'],
                                        how='left')
comp_total.to_csv(qc+'comp_total_new.csv')

#%%
pos_bda = brita.copy()
#Merge CDA information
cda_1 = cda_1[~cda_1['elasticity'].isnull()]
pos_bda_cda = pd.merge(pos_bda, cda_1[['subbrand','elasticity']],
                           left_on='Sub Brand',right_on='subbrand',how='left')
pos_bda_cda.drop(columns=['subbrand'],axis=1,inplace=True)
pos_bda_cda.to_csv(qc+'pos_bda_cda.csv')
pos_bda_cda['BPE'] = pos_bda_cda.apply(lambda x: x['elasticity'] if pd.isna(x['BPE'])
                                     else x['BPE'],axis=1)
pos_bda_cda.to_csv(qc+'pos_bda_cda_2.csv')
pos_bda_cda.drop(['elasticity'],axis = 1, inplace = True)

#%%
#Merge Cross elasticity data
pos_bpe_cross = pd.merge(pos_bda_cda, comp_total[['Channel','Products from Mapping file','Clorox Product',
'Cross Elasticity Mapping','Comp1','Cross_BP_Coeff_1','Comp2','Cross_BP_Coeff_2']], on=['Channel','Clorox Product',
'Products from Mapping file'], how='left')
pos_bpe_cross.to_csv(qc+'pos_bpe_cross.csv')

#%%
#PPL manipulation 
#Brita
ppl_raw_bri = pd.read_excel(path+'Mapping Files/Brita FY22 PP&Lfor Ranjan.xlsx','PPL_new')
ppl_raw_bri.to_csv(qc+'ppl_raw_bri.csv')
ppl_raw_bri.columns = ppl_raw_bri.columns.str.strip()
ppl_bri = ppl_raw_bri[['UPC','BU','Brand Elasticity File','Subbrand Elasticity File','Size','Volume (Msc)',
                    'BCS','Net Real','CPF Rate','TL per Unit','NCS','Contrib','Gross Profit']]         
ppl_bri_1 = ppl_bri[~ppl_bri['Subbrand Elasticity File'].isnull()]

#%%
#Combine all BU
ppl_all_bu = [ppl_bri_1]
ppl_final = reduce(lambda left,right: left.append(right, ignore_index = True), ppl_all_bu)
ppl_final['BU'] = ppl_final['BU'].str.upper()
ppl_final['Brand Elasticity File'] = ppl_final['Brand Elasticity File'].str.upper()
ppl_final['Subbrand Elasticity File'] = ppl_final['Subbrand Elasticity File'].str.upper()
ppl_final.to_csv(qc+'ppl_final.csv')

#%%
#Read cleaning list data for OI and Margin
# #Brita
margin_raw_bri = pd.read_excel(path+'Mapping Files/Brita GladMaster Item List.xlsx','cleaning list_brita')
margin_raw_bri = margin_raw_bri[['Case UPC (14 digit)','Item Description','Retail','CF Unit Disc.']]
margin_raw_bri.rename(columns={'CF Unit Disc.':'Freight Collect'},inplace=True)
margin_raw_bri['BU']="BRITA"

#%%
# For all BU 
margin_all_bu = [margin_raw_bri]
margin_bu = reduce(lambda left,right: left.append(right, ignore_index = True), margin_all_bu)
margin_bu.to_csv(qc+'margin_bu.csv')

#%%
#Pull OI information for the products in scope
oi_data = pd.merge(upc_map, margin_bu, left_on=['BU','UPC_List'], right_on=['BU','Case UPC (14 digit)'],how='left')
oi_data.to_csv(qc+'oi_data.csv')

#%%
#Merge OI and  PPNL information. Need not have 100% overlap as Arpit said not all UPCs are present both in master item or PPL.
oi_ppl = pd.merge(oi_data, ppl_final, left_on=['BU','UPC_List'], right_on=['BU','UPC'],how='left')
oi_ppl.to_csv(qc+'oi_ppl.csv')

#%%
#------Exception replacing sizes of glad with the latest size for the sub brand 
oi_ppl['Size_nw'] = oi_ppl.apply(lambda x: 40 if x['BU']=='GLAD' else x['Size'],axis=1)
oi_ppl.to_csv(qc+'oi_ppl.csv')
oi_ppl.drop(['Size'],axis=1,inplace=True)
oi_ppl.rename(columns={'Size_nw':'Size'},inplace=True)

#%%
#--------Exception Replacing the retail price of these UPC's with the latest retail price
oi_ppl['Retail_new'] = oi_ppl.apply(lambda x : 7.77 if x['Subbrand Elasticity File']=='GLAD FFO KITCHEN BAGS'
                     else (2.96 if  ((x['Subbrand Elasticity File']=='HV BOTTLED') & (x['Size']==16))
                          else (3.72 if  ((x['Subbrand Elasticity File']=='HV BOTTLED') & (x['Size']==24))
                                 else( 3.78 if ((x['Subbrand Elasticity File']=='MTBC') & (x['Size']==48))
                                      else x['Retail']))),axis=1)
oi_ppl.drop(['Retail'],axis = 1, inplace = True)
oi_ppl.rename(columns = {'Retail_new':'Retail'},inplace=True)    
oi_ppl.to_csv(qc+'oi_ppl_check.csv')

#%%
#Calculation for new Margin and Net Cost
oi_ppl['Net_cost_nw'] = oi_ppl['TL per Unit']- oi_ppl['OI']- oi_ppl['Freight Collect']
oi_ppl['Margin_nw'] = oi_ppl['Retail']/oi_ppl['Net_cost_nw'] - 1

#%%
#Aggregate PPNL and OI inforrmation
def ppl_oi(x):
    d = {}
    d['Vol'] = x['Volume (Msc)'].sum()
    d['BCS'] = np.average(x['BCS'], weights=x['Volume (Msc)'])
    d['Net Real'] = np.average(x['Net Real'], weights=x['Volume (Msc)'])
    d['CPF'] = np.average(x['CPF Rate'], weights=x['Volume (Msc)'])
    d['NCS'] = np.average(x['NCS'], weights=x['Volume (Msc)'])
    d['Contrib'] = np.average(x['Contrib'], weights=x['Volume (Msc)'])
    d['Gross Profit'] = np.average(x['Gross Profit'], weights=x['Volume (Msc)'])
    d['TL per Unit']=x['TL per Unit'].mean()
    d['OI']=x['OI'].mean()
    d['Retailer_Margin']= x['Margin_nw'].mean()       
    return pd.Series(d, index=['Vol','BCS','Net Real','CPF','NCS','Contrib','Gross Profit','TL per Unit',
                               'OI','Retailer_Margin'])
     
#%%
oi_ppl['Size'] = oi_ppl['Size'].astype(str)
oi_ppl.to_csv(qc+'oi_ppl_1.csv')
oi_ppl_2 = oi_ppl.groupby(['BU','Brand Elasticity File','Subbrand Elasticity File','Size']).apply(ppl_oi).reset_index()
oi_ppl_2.to_csv(qc+'oi_ppl_2.csv')

#%%
oi_ppl_2['Contrib Margin'] = oi_ppl_2['Contrib']/oi_ppl_2['NCS']
oi_ppl_2.to_csv(qc+'oi_ppl_2_new.csv')

#%%
#Merge PPL Mapping and PPL data
ppl_upc = pd.merge(ppl_map, ppl_final, on='UPC', how='left').reset_index(drop=True)
ppl_upc.to_csv(qc+'ppl_upc.csv')
ppl_upc.rename(columns={'BU_x':'BU'},inplace=True)
ppl_upc = ppl_upc[ppl_upc['Volume (Msc)'].notnull()].reset_index(drop=True)

#%%
def ppl_gr(x):
    d = {}
    d['Vol'] = x['Volume (Msc)'].sum()
    d['BCS'] = np.average(x['BCS'], weights=x['Volume (Msc)'])
    d['Net Real'] = np.average(x['Net Real'], weights=x['Volume (Msc)'])
    d['CPF'] = np.average(x['CPF Rate'], weights=x['Volume (Msc)'])
    d['NCS'] = np.average(x['NCS'], weights=x['Volume (Msc)'])
    d['Contrib'] = np.average(x['Contrib'], weights=x['Volume (Msc)'])
    d['Gross Profit'] = np.average(x['Gross Profit'], weights=x['Volume (Msc)'])
    d['TL per Unit']=x['TL per Unit'].mean() 
    return pd.Series(d, index=['Vol','BCS','Net Real','CPF','NCS','Contrib','Gross Profit','TL per Unit'])

#%%
ppl_upc1 = ppl_upc.groupby(['BU','Channel','Products from Mapping file','Clorox Product']).apply(ppl_gr).reset_index()
ppl_upc1['Contrib Margin'] = ppl_upc1['Contrib']/ppl_upc1['NCS']
ppl_upc1.to_csv(qc+'ppl_upc1.csv')

#%% 
pos_elas_ppl_wal = pos_bpe_cross[pos_bpe_cross['Channel'] =='WALMART CORP-RMA - WALMART'] 
pos_elas_ppl_wal['size'] = pos_elas_ppl_wal['size'].astype('str') 
pos_elas_ppl1 = pd.merge(pos_elas_ppl_wal, oi_ppl_2,left_on=['Brand','Sub Brand','size'],
                         right_on=['Brand Elasticity File', 'Subbrand Elasticity File','Size'],how='left')
pos_elas_ppl1.drop(['BU_y'], axis = 1, inplace = True)
pos_elas_ppl1.rename(columns = {'BU_x':'BU'},inplace=True)     
pos_elas_ppl1.to_csv(qc+'pos_elas_ppl1.csv')

#%% 
pos_elas_ppl_gr = pos_bpe_cross[pos_bpe_cross['Channel']!='WALMART CORP-RMA - WALMART']   
pos_elas_ppl2 = pd.merge(pos_elas_ppl_gr, ppl_upc1, on=['BU','Channel','Products from Mapping file','Clorox Product'],how='left')
pos_elas_ppl2['Retailer_Margin']=""
pos_elas_ppl2.loc[pos_elas_ppl2['Type']=='CLOROX PRODUCTS', 'Retailer_Margin' ] = (pos_elas_ppl2['Baseline Dollars']/pos_elas_ppl2['Baseline Units'])/pos_elas_ppl2['TL per Unit']-1
pos_elas_ppl2.to_csv(qc+'pos_elas_ppl2.csv')
pos_elas_ppl = pos_elas_ppl2.append(pos_elas_ppl1)
pos_elas_ppl.to_csv(qc+'pos_elas_ppl.csv')

#%%
# list of clorox products and their contribution margin
clx_list = pos_elas_ppl[pos_elas_ppl['Type'] == 'CLOROX PRODUCTS']
clx_list_1 = clx_list[['Channel','Clorox Product','Contrib Margin']]
clx_list_1["is_duplicate"] = clx_list_1.duplicated()
clx_list_2 = clx_list_1[clx_list_1['is_duplicate']==False]
clx_list_2 = clx_list_2.drop(['is_duplicate'], axis=1)
clx_list_2 = clx_list_2[clx_list_2['Contrib Margin'].notnull()]

#%%
#Get contribution index for all manufacturers
pos_elas_ppl_contrib = pd.merge(pos_elas_ppl, contrib_factor, on='Manufacturer', how='left')
pos_elas_ppl_contrib.to_csv(qc+'pos_elas_ppl_contrib.csv')

#%%
final = pd.merge(pos_elas_ppl_contrib, clx_list_2,left_on=['Channel','Clorox Product'], right_on=['Channel','Clorox Product'], how='left')
final.to_csv(qc+'final.csv')

#%%
final['Contrib Margin New'] = final['Contrib Margin_y']*final['Index to Clorox']
final.drop(['Contrib Margin_x','Contrib Margin_y'], axis = 1, inplace = True)
final.rename(columns = {'Contrib Margin New':'Contrib Margin'}, inplace=True)     
final.to_csv(qc+'final.csv')

#%%
# Category Aggregation
cat_total['Geography'] = cat_total['Geography'].str.upper().str.strip() 
final_catmap = pd.merge(final, cat_map, on=['Channel','Products from Mapping file'],how='left')
final_cat_tot = pd.merge(final_catmap, cat_total, left_on=['Channel','Category Name'],
                       right_on=['Geography','Category Name'],how='left')
final_cat_tot.drop(['BU_y','Type_y','Geography_x','Geography_y'],axis = 1, inplace = True)
final_cat_tot.rename(columns={'BU_x':'BU','Channel_x':'Channel','Type_x':'Type',
                              'Clorox Sub Category Value':'Clorox Sub Category Value_to_delete',
                              'Category Name':'Clorox Sub Category Value'}, inplace = True)
final_cat_tot.to_csv(qc+'final_cat_tot.csv')

#%%
seg_total['Geography'] = seg_total['Geography'].str.upper().str.strip()
final_cat_seg = pd.merge(final_cat_tot, seg_total, left_on=['Channel','Clorox Segment Value'],
                       right_on=['Geography','Clorox Segment Value'],how='left')
final_cat_seg.drop(columns=['Geography'],inplace=True)
final_cat_seg.to_csv(qc+'final_cat_seg.csv')

#%%
final_cat_seg['Time_Period'] = "Latest 52 Weeks Ending 03-27-22"

#%%
print(final_cat_seg.columns)

#%%
final_cat_seg['Clorox Sub Category Value_to_delete']=""
final_cat_seg['Clorox Brand Value']=""
final_cat_seg['POS Products']=""
final_cat_seg = final_cat_seg[['BU','Channel','Type','Products from Mapping file','POS Products','Clorox Product','Brand',
'Sub Brand','size','Manufacturer','Clorox Brand Value',
'Clorox Sub Category Value_to_delete','Clorox Segment Value','Standard Hierarchy Level','SCBV',
'Stat Case Volume','Unit Sales','Dollar Sales','Volume Sales','Baseline Dollars',
'Baseline Units','Baseline Volume','BPE','Cross Elasticity Mapping','Comp1','Cross_BP_Coeff_1',
'Comp2','Cross_BP_Coeff_2','Brand Elasticity File','Subbrand Elasticity File',
'Size','Vol','BCS','Net Real','CPF','NCS','Contrib','Gross Profit','TL per Unit','OI','Retailer_Margin',
'Index to Clorox','Contrib Margin','Clorox Sub Category Value','Cat_Tot SCBV','Cat_Tot Stat Case Volume',
'Cat_Tot Unit Sales','Cat_Tot Dollar Sales','Cat_Tot Volume Sales',
'Cat_Tot Baseline Dollars','Cat_Tot Baseline Units',
'Cat_Tot Baseline Volume','Seg_Tot SCBV',
'Seg_Tot Stat Case Volume','Seg_Tot Unit Sales',
'Seg_Tot Dollar Sales','Seg_Tot Volume Sales',
'Seg_Tot Baseline Dollars','Seg_Tot Baseline Units',
'Seg_Tot Baseline Volume','Time_Period']]

#%%
# Use this file in the to run the optimal scenario macro file
# Delete blank PPLs for Clorox
final_cat_seg.to_csv(qc+'final_output_FY21Q4_Brita.csv',index=False)

#%%
df_clx = final_cat_seg[final_cat_seg['Type']=='CLOROX PRODUCTS']
df_b = final_cat_seg[final_cat_seg['Type']=='BRANDED PRODUCTS']
df_pl = final_cat_seg[final_cat_seg['Type']=='PRIVATE LABEL']

#%%
cols = ['Type','Products from Mapping file','POS Products','Brand','Sub Brand',
    'size','Manufacturer','Clorox Brand Value','Clorox Segment Value','Standard Hierarchy Level',
    'SCBV','Stat Case Volume','Unit Sales','Dollar Sales','Volume Sales',
    'Baseline Dollars','Baseline Units','Baseline Volume','BPE',
    'Cross Elasticity Mapping','Comp1','Cross_BP_Coeff_1',
    'Comp2','Cross_BP_Coeff_2','Brand Elasticity File','Subbrand Elasticity File',
    'Size','Vol','BCS','Net Real','CPF','NCS','Contrib','Gross Profit','TL per Unit','OI',
    'Retailer_Margin','Index to Clorox','Contrib Margin','Cat_Tot SCBV','Cat_Tot Stat Case Volume',
    'Cat_Tot Unit Sales','Cat_Tot Dollar Sales','Cat_Tot Volume Sales','Cat_Tot Baseline Dollars',
    'Cat_Tot Baseline Units','Cat_Tot Baseline Volume','Seg_Tot SCBV', 'Seg_Tot Stat Case Volume',
    'Seg_Tot Unit Sales','Seg_Tot SCBV','Seg_Tot Stat Case Volume', 'Seg_Tot Unit Sales','Seg_Tot Dollar Sales',
    'Seg_Tot Volume Sales','Seg_Tot Baseline Dollars','Seg_Tot Baseline Units','Seg_Tot Baseline Volume']

df_clx.columns = np.where(df_clx.columns.isin(cols), df_clx.columns+"_CL", df_clx.columns)
df_clx.to_csv(qc+'df_clx.csv')

#%%
df_b.columns = np.where(df_b.columns.isin(cols), df_b.columns+"_B", df_b.columns)
df_b.drop(columns=['Vol_B','BCS_B','Net Real_B','CPF_B','NCS_B','Contrib_B','Gross Profit_B',
                   'TL per Unit_B','Retailer_Margin_B','Cat_Tot SCBV_B','Cat_Tot Stat Case Volume_B',
                   'Cat_Tot Unit Sales_B','Cat_Tot Dollar Sales_B',
                    'Cat_Tot Volume Sales_B','Cat_Tot Baseline Dollars_B','Cat_Tot Baseline Units_B',
                    'Cat_Tot Baseline Volume_B','Seg_Tot SCBV_B','Seg_Tot Stat Case Volume_B',
'Seg_Tot Unit Sales_B','Seg_Tot Dollar Sales_B','Seg_Tot Volume Sales_B',
                    'Seg_Tot Baseline Dollars_B','Seg_Tot Baseline Units_B','Seg_Tot Baseline Volume_B',
                    ],axis=1,inplace=True)
df_b.to_csv(qc+'df_b.csv')

#%%
df_pl.columns = np.where(df_pl.columns.isin(cols), df_pl.columns+"_PL", df_pl.columns)
df_pl.drop(columns=['Vol_PL','BCS_PL','Net Real_PL','CPF_PL','NCS_PL','Contrib_PL',
                    'Gross Profit_PL','TL per Unit_PL',
                   'Retailer_Margin_PL','Cat_Tot SCBV_PL','Cat_Tot Stat Case Volume_PL',
                   'Cat_Tot Unit Sales_PL','Cat_Tot Dollar Sales_PL','Cat_Tot Volume Sales_PL',
                    'Cat_Tot Baseline Dollars_PL','Cat_Tot Baseline Units_PL',
                    'Cat_Tot Baseline Volume_PL',                   
                    'Seg_Tot SCBV_PL','Seg_Tot Stat Case Volume_PL',
                    'Seg_Tot Unit Sales_PL','Seg_Tot Dollar Sales_PL','Seg_Tot Volume Sales_PL',
                    'Seg_Tot Baseline Dollars_PL','Seg_Tot Baseline Units_PL',
                    'Seg_Tot Baseline Volume_PL',],axis=1,inplace=True)
df_pl.to_csv(qc+'df_pl.csv')

#%%
df_clx_b=pd.merge(df_clx,df_b, on=['BU','Channel','Clorox Product','Clorox Sub Category Value','Time_Period',],how='left')
df_clx_b.to_csv(qc+'df_clx_b.csv')

#%%
df_final=pd.merge(df_clx_b,df_pl, on=['BU','Channel','Clorox Product','Clorox Sub Category Value','Time_Period',],how='left')
df_final.to_csv(qc+'df_final.csv')

#%%
df_final.loc[df_final['Type_CL']=='CLOROX PRODUCTS','Type_CL']='CLOROX'
df_final.loc[df_final['Type_B']=='BRANDED PRODUCTS','Type_B']='BRANDED'
df_final.loc[df_final['Type_PL']=='PRIVATE LABEL','Type_PL']='PL'
df_final.to_csv(qc+'df_final_Brita.csv')
