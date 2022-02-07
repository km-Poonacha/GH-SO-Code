# -*- coding: utf-8 -*-
"""
Created on Sat Jan 22 13:06:31 2022

@author: pmedappa
"""

import pandas as pd

India= r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\GH_MonthPanel_AllPanel_0122_fulldata.xlsx'
China= r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\GH_MonthPanel_CF_data_ICMT.xlsx'
# Mexico= r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\Counterfactual\GH_MonthPanel_CF_mexico_data.xlsx'
# Turkey= r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\Counterfactual\GH_MonthPanel_CF_turkey_data.xlsx'
w_xl = r"C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\GH_MonthPanel_Fulldata.xlsx"

df_i = pd.read_excel(India,header= 0)
print(df_i.shape)
df_i =df_i.drop(['company'], axis = 1)
df_c = pd.read_excel(China,header= 0)
df_c =df_c.drop(['sponsorsListing_shortDescription','sponsorsListing_name'], axis = 1)

print(df_c.shape)
# df_m = pd.read_excel(Mexico,header= 0)
# df_t = pd.read_excel(Turkey,header= 0)

df_i = df_i.append([df_c], ignore_index=True)
df_i =df_i.drop(['company'], axis = 1)
print(df_i.shape)
df_i.to_excel(w_xl , index = False)