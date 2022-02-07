# -*- coding: utf-8 -*-
"""
Created on Sun Dec 26 23:45:56 2021

@author: pmedappa
"""
import pandas as pd
import ast





def mergepanel():
    left = r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\GH_MonthPanel_AllPanel_1221.xlsx'       
    right = r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\GH_MonthPanel_0122.xlsx'
    MERGE = r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\GH_MonthPanel_AllPanel_0122.xlsx'
    test = r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\test.xlsx'
    df_l = pd.read_excel(left,header= 0)
    print(df_l.shape)
    df_r = pd.read_excel(right,header= 0)
    print(df_r.shape)
    df_m = df_l.merge(df_r[['login','month','subs_added']], left_on=['login','month'], right_on=['login','month'])
    
    login_ids_l = df_l.login.unique()
    login_ids_r = df_r.login.unique()
    df_t = pd.concat([pd.Series(login_ids_l), pd.Series(login_ids_r) ], ignore_index = True)
    # df_m['location'] = 'china'
    
    # df_m =df_m.drop(['github username','Unnamed: 0_x','Unnamed: 0_y', 'match confirm'], axis = 1)
    # df_m =df_m.drop(['stack ID_y','stack url_y'], axis = 1)
    
    
    # df_m =df_m.drop(['stack url','bio','sponsor_created_2018yearmonth','email','sponsorsListing_tiers_edges','sponsorshipsAsMaintainer_nodes'], axis = 1)
    print(df_m.shape)
    df_m.to_excel(MERGE, index = False)  
    df_t.to_excel(test, index = False)  
    
def merge():
    left = r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\GH_MonthPanel_AllPanel_0122.xlsx'     
    right = r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\CleanConsolidatedSponsors_SOMatchOnly_Sub0122_SOMatch3.xlsx'
    right2 = r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\CleanConsolidatedSponsors_SOMatchOnly_Sub0121_SOMatch.xlsx'
    MERGE = r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\GH_MonthPanel_AllPanel_0122_fulldata.xlsx'
    df_l = pd.read_excel(left,header= 0)
    print(df_l.shape)
    df_r = pd.read_excel(right,header= 0)
    df_r2 = pd.read_excel(right2,header= 0)
    df_r =df_r.drop(['stack url','sponsorshipsAsMaintainer_nodes_0122','sponsorsListing_tiers_edges_0122','sponsorsListing_createdAt','stack ID'], axis = 1)
    df_r = df_r.groupby(by=["login"]).first()
    print(df_r.shape)
    df_m = df_l.merge(df_r,how='left', left_on='login', right_on='login')
    
    df_m = df_m.merge(df_r2[['location','stack ID','name','company','createdAt']], left_on='stack ID', right_on='stack ID',how='left')
    # df_m =df_m.drop(['stack ID_y','stack url_y'], axis = 1)
    df_m['treated_STRIPE'] = 1
    print(df_m.shape)
    df_m.to_excel(MERGE, index = False)  
    
merge()