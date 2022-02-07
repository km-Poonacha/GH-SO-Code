# -*- coding: utf-8 -*-
"""
Created on Mon Dec 20 21:59:29 2021

@author: pmedappa
"""




def main():

    r_spon_xl = r"C:\Users\pmedappa\Dropbox\Data\Sponsors\GH_MonthPanel.xlsx"
    r_ans_xl = r"C:\Users\pmedappa\Dropbox\Data\Sponsors\GH_MonthPanel.xlsx"
    w_spon_xl = r"C:\Users\pmedappa\Dropbox\Data\Sponsors\SOGH_MonthPanel_Ans.xlsx"

    
    
    df_spon = pd.read_excel(r_spon_xl,header= 0)
    df_spon_panel = pd.DataFrame()
    df_spon_panel.to_excel(w_spon_xl, index = False) 
    
    df_spon_exp = expandSponsors(df_spon)
    df_spon_exp.to_excel(w_spon_xl , index = False)
    df_spon_panel = createPanel(df_spon_exp)
    
    stack_ids = df_spon_panel['stack ID'].unique()
    print("Total stack ids : ", len(login_ids))
    df_month_panel = pd.DataFrame()
    for login in stack_ids:
        print(login)
        df_temp = df_spon_panel[df_spon_panel['login']== login]
        df_month_temp = MonthPanel(df_temp, login )
        df_month_panel = df_month_panel.append(df_month_temp,  ignore_index = True)
        
    df_month_panel.to_excel(w_spon_xl_mp , index = False)
   
    
main()