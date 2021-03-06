# -*- coding: utf-8 -*-
"""
Created on Wed Sep 11 17:04:10 2019

@author: pmedappa
"""

import sys
if r"C:\Users\pmedappa\Dropbox\Code\CustomLib\PooLib" not in sys.path:
    sys.path.append(r'C:\Users\pmedappa\Dropbox\Code\CustomLib\PooLib')
    print(sys.path)
from poo_ghmodules import getGitHubapi
from poo_ghmodules import ghpaginate
from poo_ghmodules import ghparse_row
from poo_ghmodules import gettoken
import pandas as pd
import numpy as np
import requests
from time import sleep
import ast
MAX_ROWS_PERWRITE = 100

DF_REPO = pd.DataFrame()
DF_COUNT = 0

LOG_CSV = r'C:\\Users\pmedappa\Dropbox\Course and Research Sharing\Research\Data\Sponsor\Rand\UserSpon_log.csv'


def appendrowindf(user_xl, row):
    """This code appends a row into the dataframe and returns the updated dataframe"""
    global DF_REPO 
    global DF_COUNT
    DF_REPO= DF_REPO.append(row, ignore_index = True, sort=False)
    DF_COUNT = DF_COUNT + 1
    if DF_COUNT >= MAX_ROWS_PERWRITE :
        df = pd.read_excel(user_xl,header= 0)
        df= df.append(DF_REPO, ignore_index = True, sort=False)
        df.to_excel(user_xl, index = False) 
        DF_COUNT = 0
        DF_REPO = pd.DataFrame()


def run_query(user_row,user_xl): 
    """ A simple function to use requests.post to make the API call. Note the json= section."""
    TOKEN = gettoken(r"C:\Users\pmedappa\Dropbox\Code\PW\GHtoken.txt")
    headers = {"Authorization": "Bearer "+ TOKEN } 
    end = False
    query = """ 
    query {
          user(login: \""""+str(user_row["login"])+"""\") {
                email
                location
                isHireable
                followers {
                  totalCount
                }
                following {
                  totalCount
                }
                repositories {
                  totalCount
                }
                sponsorsListing {
                  createdAt
                  shortDescription
                  name
                  tiers(first: 100) {
                    totalCount
                    edges {
                      node {
                        name
                        description
                        monthlyPriceInDollars
                        updatedAt
                      }
                    }
                  }
                }
                sponsorshipsAsMaintainer(first: 100) {
                  totalCount
                  pageInfo {
                      hasNextPage
                      startCursor
                      endCursor
                      }
    
                  nodes {
                    createdAt
                    sponsor {
                      login
                    }
                  }
                }
          }
        }"""    
    try:
        request = requests.post('https://api.github.com/graphql', json={'query':query}, headers=headers)
        req_json = request.json()
        # endc = req_json['data']['user']['sponsorshipsAsMaintainer']['pageInfo']['startCursor']
        print(req_json['data']['user']['sponsorshipsAsMaintainer']['totalCount'])
        # print(endc)
    except:
        print("Error getting starting cursor")
        print(req_json)
        temp_row = list()
        temp_row.append("DELETED")
        temp_row.append("DELETED")
        temp_row.append("DELETED")
        temp_row.append("DELETED")
        temp_row.append("DELETED")
        temp_row.append("DELETED")
        temp_row.append("DELETED")
        temp_row.append("DELETED")
        temp_row.append("DELETED")
        user_row_temp= user_row[['login','stack ID','stack url']].append(pd.Series(temp_row), ignore_index = True)
        appendrowindf(user_xl, user_row_temp) 
        return 404
    parse_query_response(req_json,user_row,user_xl)
    if req_json['data']['user']['sponsorshipsAsMaintainer']['pageInfo']['hasNextPage']:     
        endc = req_json['data']['user']['sponsorshipsAsMaintainer']['pageInfo']['endCursor']
    else:
        end = True

    while not end:
        query = """
            query($cursor:String!){
              rateLimit {
                cost
                remaining
                resetAt
              }
              user(login: \""""+str(user_row["login"])+"""\") {
                email
                location
                isHireable
                followers {
                  totalCount
                }
                following {
                  totalCount
                }
                repositories {
                  totalCount
                }
                sponsorsListing {
                  createdAt
                  shortDescription
                  name
                  tiers(first: 100) {
                    totalCount
                    edges {
                      node {
                        name
                        description
                        monthlyPriceInDollars
                        updatedAt
                      }
                    }
                  }
                }
                sponsorshipsAsMaintainer(first: 100, after:$cursor) {
                  totalCount
                  pageInfo {
                      hasNextPage
                      startCursor
                      endCursor
                      }
    
                  nodes {
                    createdAt
                    sponsor {
                      login
                    }
                  }
                }
              }
            }"""           
        
        variables = {
             "cursor" : endc
             }
        body = {
                "query": query,
                "variables": variables
                    }
        try:
            request = requests.post('https://api.github.com/graphql', json=body, headers=headers)
            req_json = request.json()
            print(req_json['data']['rateLimit']['remaining'])

            if int(req_json['data']['rateLimit']['remaining']) <100:
                print("sleeping ........")
                sleep(60)           
        except:
            print("Error running graphql")
            temp_row = list()
            temp_row.append("")
            temp_row.append("")
            temp_row.append("")
            temp_row.append("")
            temp_row.append("")
            temp_row.append("")
            temp_row.append("")
            temp_row.append("")
            temp_row.append("")
            user_row_temp= user_row[['login','stack ID','stack url']].append(pd.Series(temp_row), ignore_index = True)
            appendrowindf(user_xl, user_row_temp)
            return 404
        

        parse_query_response(req_json,user_row,user_xl)
        
        if req_json['data']['user']['sponsorshipsAsMaintainer']['pageInfo']['hasNextPage']:     
            endc = req_json['data']['user']['sponsorshipsAsMaintainer']['pageInfo']['endCursor']
        else:
            end = True

    return 0

def parse_query_response(req_json,user_row,user_xl):
    temp_row = list()
        
    try:
        userdetails =  req_json['data']['user']
        temp_row.append(userdetails['isHireable'])
        temp_row.append(userdetails['followers']['totalCount'])
        temp_row.append(userdetails['following']['totalCount'])
        temp_row.append(userdetails['repositories']['totalCount'])
    except: 
        temp_row.append("")
        temp_row.append("") 
        temp_row.append("")
        temp_row.append("")             
        
        
    try:         
        sponsorl = req_json['data']['user']['sponsorsListing']
        temp_row.append(sponsorl['createdAt'])            
        temp_row.append(sponsorl['tiers']['totalCount'])
        temp_row.append(sponsorl['tiers']['edges'])
    except: 
        temp_row.append("")
        temp_row.append("")
        temp_row.append("") 
    
    try:
        sponsorasm = req_json['data']['user']['sponsorshipsAsMaintainer']
        
        temp_row.append(sponsorasm['totalCount'])
        temp_row.append(sponsorasm ['nodes'])
        print("Collected ",len(sponsorasm ['nodes']))
    except:
        
        temp_row.append("")
        temp_row.append("")
    user_row_temp= user_row[['login','stack ID','stack url']].append(pd.Series(temp_row), ignore_index = True)
    appendrowindf(user_xl, user_row_temp)  
    
def collapse_sponsors(user_df):
      user_df = user_df.groupby(['login'])['sponsorshipsAsMaintainer_nodes_12_2_21'].apply(list).reset_index()
      user_df['Sponsor_Check'] = user_df['sponsorshipsAsMaintainer_nodes_12_2_21'].apply(len)
      return user_df
    
def main():
    """Main function"""   
    global DF_REPO 
    global DF_COUNT
    r_user_xl = r'C:\Users\pmedappa\Dropbox\Data\Sponsors\CleanConsolidatedSponsors_SOMatchOnly_Sub0121_SOMatch.xlsx'
    w_user_xl = r'C:\Users\pmedappa\Dropbox\Data\Sponsors\CleanConsolidatedSponsors_SOMatchOnly_Sub0122_SOMatch3.xlsx'
    user_df = pd.read_excel(r_user_xl,header= 0)
    df_test = pd.DataFrame()
    df_test.to_excel(w_user_xl, index = False) 
    # col = user_df.columns
    col = pd.Index(["login",'stack ID','stack url','isHireable_0122','followers_0122','following_0122','repositories_0122',"sponsorsListing_createdAt","sponsorsListing_tiers_totalCount_0122","sponsorsListing_tiers_edges_0122","sponsorshipsAsMaintainer_totalCount_0122","sponsorshipsAsMaintainer_nodes_0122"])

    # user_df = collapse_sponsors(user_df)
    # user_df.to_excel(w_user_xl, index = False) 
    # return
    for i,row in user_df.iterrows():
        print(row['login'])
        run_query(row,w_user_xl)
    

    df = pd.read_excel(w_user_xl,header= 0)
    df= df.append(DF_REPO, ignore_index = True, sort=False)

    df.columns = col
    

    df.to_excel(w_user_xl, index = False) 
    DF_COUNT = 0
    DF_REPO = pd.DataFrame()

main()