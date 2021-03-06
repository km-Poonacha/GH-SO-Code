# -*- coding: utf-8 -*-
"""
Created on Sat Dec 18 19:10:02 2021

@author: pmedappa
"""

import pandas as pd
import numpy as np
import json
import ast


def expandSponsors(df_spon):
    """ Expand the contributor info to full contributor - sponsor inf.
    Returns expanded sponsor info """
    df_spon_exp = pd.DataFrame()
    for i,row in df_spon.iterrows():
        
        df_spon_panel_temp = pd.DataFrame()
        if ((pd.notna(row['sponsorshipsAsMaintainer_nodes'])) and (len(ast.literal_eval(row['sponsorshipsAsMaintainer_nodes'])) > 0)):
            for sub in ast.literal_eval(row['sponsorshipsAsMaintainer_nodes']):

                if sub['sponsor']:
                    spon_login = dict(sub['sponsor'])['login']
                else:
                    
                    spon_login = ""
                df_spon_panel_temp =  row[['login','sponsorsListing_createdAt','stack ID']].to_frame().T
                df_spon_panel_temp.reset_index(drop=True, inplace=True)
                df_spon_panel_temp = pd.concat([df_spon_panel_temp, pd.DataFrame([[sub['createdAt'],spon_login]], columns = ['sponsor_createdDate','sponsor_login'])], axis=1)
                df_spon_exp = df_spon_exp.append(df_spon_panel_temp)
                
        else:
            
            spon_login = ""
            df_spon_panel_temp =  row[['login','sponsorsListing_createdAt','stack ID']].to_frame().T
            df_spon_panel_temp.reset_index(drop=True, inplace=True)
            df_spon_panel_temp = pd.concat([df_spon_panel_temp, pd.DataFrame([["",spon_login]], columns = ['sponsor_createdDate','sponsor_login'])], axis=1)
            df_spon_exp = df_spon_exp.append(df_spon_panel_temp)

            
    return df_spon_exp
    
    
    
def createPanel(df_spon_exp):
    """Split date into year month and day columns, aggregate month strating from 2008-01-01"""
    # Sponsor List date
    df_spon_exp['sponsorsListing_year'] = pd.to_numeric(df_spon_exp['sponsorsListing_createdAt'].str.split('-').str[0])
    df_spon_exp['sponsorsListing_month'] = pd.to_numeric(df_spon_exp['sponsorsListing_createdAt'].str.split('-').str[1])
    
    df_spon_exp['sponsorsListing_2018yearmonth'] = (df_spon_exp['sponsorsListing_year'] - 2018) *12 + df_spon_exp['sponsorsListing_month'] 

    #Sponsor Created Date

    df_spon_exp['sponsor_createdYear'] = pd.to_numeric(df_spon_exp['sponsor_createdDate'].str.split('-').str[0])
    df_spon_exp['sponsor_createdMonth'] = pd.to_numeric(df_spon_exp['sponsor_createdDate'].str.split('-').str[1])
    
    df_spon_exp['sponsor_created_2018yearmonth'] = (df_spon_exp['sponsor_createdYear'] - 2018) *12 + df_spon_exp['sponsor_createdMonth']         
   
    return df_spon_exp

def MonthPanel(df_temp, login):
    """ Month groupby for the number of contributors"""
    sponsorsListing_2018yearmonth = df_temp['sponsorsListing_2018yearmonth'].unique().squeeze()
    sponsorsListing_createdAt = df_temp['sponsorsListing_createdAt'].unique().squeeze()
    stack_ID = df_temp['stack ID'].unique().squeeze()

    # print(stack_ID)
    df2 = pd.DataFrame({'month' : [i for i in range(1,49)]}) #2018 - 2021; 0-47
    df2['c_month'] = df2['month']%12
    df2['c_month'] = df2['c_month'].replace(0,12)

    df2['c_year'] = np.nan
    df2['c_year'] = np.where(( df2['month'] <= 12), 2018, df2['c_year'])
    df2['c_year'] = np.where(( (df2['month'] > 12) & (df2['month'] <= 24)), 2019, df2['c_year'])
    df2['c_year'] = np.where(( (df2['month'] > 24) & (df2['month'] <= 36)), 2020, df2['c_year'])
    df2['c_year'] = np.where(( (df2['month'] > 36) & (df2['month'] <= 48)), 2021, df2['c_year'])

    
    df3  = df_temp.groupby('sponsor_created_2018yearmonth')[['login']].count()
    df3 = df3.reset_index().rename(columns={"login": "subs_added"})


    df2 = df2.merge(df3, left_on='month', right_on='sponsor_created_2018yearmonth' ,how ='left')
    df2['login'] = login
    df2['sponsorsListing_2018yearmonth'] = sponsorsListing_2018yearmonth
    df2['sponsorsListing_createdAt'] = sponsorsListing_createdAt
    df2['stack ID'] = stack_ID

    
    
    df2['subs_added'] = df2['subs_added'].fillna(0)
    
    df2['sponsor_listed'] = np.where(( df2['month'] >= df2['sponsorsListing_2018yearmonth']), 1, 0)
 
    return df2


def main():

    r_spon_xl = r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\Counterfactual\CleanConsolidatedSponsors_mexico_DirtyCounterfactual_SOMatch_confirm_data.xlsx'

    w_spon_xl = r"C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\Counterfactual\GH_Panel_CF_mexico.xlsx"
    w_spon_xl_mp = r"C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\Counterfactual\GH_MonthPanel_CF_mexico.xlsx"
    
    
    df_spon = pd.read_excel(r_spon_xl,header= 0)
    
    df_spon_panel = pd.DataFrame()
    df_spon_panel.to_excel(w_spon_xl, index = False) 
    
    df_spon_exp = expandSponsors(df_spon)
    df_spon_exp.to_excel(w_spon_xl , index = False)
    df_spon_panel = createPanel(df_spon_exp)
    
    login_ids = df_spon_panel.login.unique()
    
    
    df_month_panel = pd.DataFrame()
    for login in login_ids:
        print(login)
        df_temp = df_spon_panel[df_spon_panel['login']== login]
        df_month_temp = MonthPanel(df_temp, login )
        df_month_panel = df_month_panel.append(df_month_temp,  ignore_index = True)
    print("Total login ids : ", len(login_ids))    
    df_month_panel.to_excel(w_spon_xl_mp , index = False)
   
    
main()