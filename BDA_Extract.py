# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

# -*- coding: utf-8 -*-

import pyodbc
import pandas as pd
import numpy as np
import re

#%%
conn1 = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=D:\Work\TEG Analytics\Clorox Pricing\OneDrive_1_21-6-2021\Pricing\FY22Q3\BDA Co-Efficient File/Hyperion_CoefficientRep_CY2021_KFD.mdb;')
#conn2 = pyodbc.connect(r'Driver={Microsoft Access Driver(*.mdb,*.accdb)};DBQ=D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Automation/Hyperion Databases for ElasticitiesElasticties/Hyperion_CoefficientRep_CY2019.mdb;')
#conn3 = pyodbc.connect(r'Driver={Microsoft Access Driver(*.mdb,*.accdb)};DBQ=D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Automation/Hyperion Databases for ElasticitiesElasticties/Hyperion_CoefficientRep_CY2018.mdb;')
#conn4 = pyodbc.connect(r'Driver={Microsoft Access Driver(*.mdb,*.accdb)};DBQ=D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Automation/Hyperion Databases for ElasticitiesElasticties/Hyperion_CoefficientRep_CY2017.mdb;')

#conn1 = pyodbc.connect(r'D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Automation/Hyperion Databases for ElasticitiesElasticties/Hyperion_CoefficientRep_CY20201.mdb;')
#conn2 = pyodbc.connect(r'D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Automation/Hyperion Databases for ElasticitiesElasticties/Hyperion_CoefficientRep_CY2019.mdb;')
#conn3 = pyodbc.connect(r'D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Automation/Hyperion Databases for ElasticitiesElasticties/Hyperion_CoefficientRep_CY2018.mdb;')
#conn4 = pyodbc.connect(r'D:/Work/TEG Analytics/Clorox Pricing/OneDrive_1_21-6-2021/Automation/Hyperion Databases for ElasticitiesElasticties/Hyperion_CoefficientRep_CY2017.mdb;')

#%%
def extract(conn):
    cur = conn.cursor()
    a = []
    for row in cur.tables():
        a.append(row.table_name)
    tab = str(a[len(a)-1])
    query = 'Select * from '+tab
    print(query)
    return pd.read_sql(query, conn)

#%%
df1 = extract(conn1)
# df2 = extract(conn2)
# df3 = extract(conn3)
# df4 = extract(conn4)

#%%
#final_db = df1.append([df2, df3, df4])

#%%
#Update this code to get data from SQL database and all products at Product Level= S ---final_db = df1.append([df2, df3, df4])
#final_db1 = final_db.drop_duplicates()

#%%
conn1.close()
# conn2.close()
# conn3.close()
# conn4.close()

#%%
