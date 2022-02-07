# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 11:58:14 2020

@author: pmedappa
"""
import pandas as pd
import ast

consolidate_sponsors = r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\Counterfactual\UserSponsor_turkey_DirtyCounterfactual.xlsx'       
new_consolidate_sponsors = r'C:\Users\pmedappa\Dropbox\Research\GH-SO\GH-SO-Project\Data\Counterfactual\CleanUserSponsor_turkey_DirtyCounterfactual.xlsx'       

df = pd.read_excel(consolidate_sponsors,header= 0)
print(df.shape)
df = df.drop_duplicates(subset = [0])
print(df.shape)
print(df[df[16] > 0].shape)
# print(df[(df[17].transform(ast.literal_eval).str.len() > 0)].shape)
print(df[16].sum())


df.columns=["login", "name", "email", "company", "bio", "location",
                            "createdAt", "isHireable", "followers_totalCount", "following_totalCount","repositories_totalCount",
                            "sponsorsListing_createdAt","sponsorsListing_shortDescription","sponsorsListing_name",
                            "sponsorsListing_tiers_totalCount","sponsorsListing_tiers_edges","sponsorshipsAsMaintainer_totalCount",
                            "sponsorshipsAsMaintainer_nodes"]

df.to_excel(new_consolidate_sponsors) 
 