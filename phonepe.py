#Importing Libraries
import json
import pandas as pd
import os
import pymysql
from sqlalchemy import create_engine 
from tabulate import tabulate
import streamlit as st
from PIL import Image
import plotly.express as px
from git.repo.base import Repo

#MySQlConnection

import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Moni1234", 
)
mycursor = mydb.cursor(buffered=True)
Engine=create_engine("mysql+pymysql://root:Moni1234@localhost/phonepe")

#Primary Program

#Repo.clone_from("https://github.com/PhonePe/pulse.git","C:/Users/Admin/Desktop/Phonepes")

def finalfun():
    a1=aggregate_transact()
    a2=aggregate_user()
    b1=map_transact()
    b2=map_user()
    c1=top_transact()
    c2=top_user()
def aggregate_transact():
    
    try:
        mycursor.execute("use  phonepe")
    except:
        mycursor.execute("create database phonepe")
        mycursor.execute("use  phonepe")
        
    path_1 = "C:/Users/Admin/pulse/data/aggregated/transaction/country/india/state/"
    Agg_tran_state_list = os.listdir(path_1)
    Agg_tran_state_list
    ls=[]
    for i in Agg_tran_state_list:
        p=path_1+i+"/"
        agg_year=os.listdir(p)
        for j in agg_year:
            p_y=p+j+"/"
            files=os.listdir(p_y)
            for file in files:
                with open(p_y+file) as json_file:
                    data=json.load(json_file)
                    for k in data["data"]["transactionData"]:
                        final=dict(Year=int(j),
                                   state=i,
                                   paymenttype=k["name"],
                                   count=int(k["paymentInstruments"][0]["count"]),
                                   amount="{:.2f}".format(k["paymentInstruments"][0]["amount"]),
                                   quarter=int(file.strip('.json')))                                  
                        ls.append(final)
    df=pd.DataFrame(ls)
    df["amount"].astype(float)
    df.to_sql("agg_t",Engine,if_exists="append",index=False)
    
                        
def aggregate_user():
    path_1 = "C:/Users/Admin/pulse/data/aggregated/user/country/india/state/"
    Agg_tran_state_list = os.listdir(path_1)
    Agg_tran_state_list
    ls=[]
    for i in Agg_tran_state_list:
        p=path_1+i+"/"
        agg_year=os.listdir(p)
        for j in agg_year:
            p_y=p+j+"/"
            files=os.listdir(p_y)
            for file in files:
                with open(p_y+file) as json_file:
                    data=json.load(json_file)
                    final=dict(Year=int(j),
                               state=i,
                               registeredUsers=int(data["data"]["aggregated"]["registeredUsers"]),
                               appOpens=int(data["data"]["aggregated"]["appOpens"]),
                               quarter=int(file.strip('.json')))
                    ls.append(final)
    df=pd.DataFrame(ls)
    df.to_sql("agg_u",Engine,if_exists="append",index=False)

def map_transact():
    path_1 = "C:/Users/Admin/pulse/data/map/transaction/hover/country/india/state/"
    map_tran_state_list = os.listdir(path_1)
    map_tran_state_list
    ls=[]
    for i in map_tran_state_list:
        p=path_1+i+"/"
        map_year=os.listdir(p)
        for j in map_year:
            p_y=p+j+"/"
            files=os.listdir(p_y)
            for file in files:
                with open(p_y+file) as json_file:
                    data=json.load(json_file)
                    for k in data["data"]["hoverDataList"]:
                        final=dict(Year=int(j),
                                   state=i,
                                   district_name=k["name"],
                                   count=int(k["metric"][0]["count"]),
                                   amount="{:.2f}".format(k["metric"][0]["amount"]),
                                   quarter=int(file.strip('.json')))
                        ls.append(final)
    df=pd.DataFrame(ls)
    df["amount"].astype(float)
    df.to_sql("map_t",Engine,if_exists="append",index=False)

def map_user():
    path_1 = "C:/Users/Admin/pulse/data/map/user/hover/country/india/state/"
    map_user_state_list = os.listdir(path_1)
    map_user_state_list
    ls=[]
    for i in map_user_state_list:
        p=path_1+i+"/"
        map_year=os.listdir(p)
        for j in map_year:
            p_y=p+j+"/"
            files=os.listdir(p_y)
            for file in files:
                with open(p_y+file) as json_file:
                    data=json.load(json_file)
                    for k in data["data"]["hoverData"].items():
                        final=dict(Year=int(j),
                                   state=i,
                                   district_name=k[0],
                                   registereduser =int(k[1]["registeredUsers"]),
                                   appopens=int(k[1]["appOpens"]),
                                   quarter=int(file.strip('.json')))
                        ls.append(final)
                    
                    
    df=pd.DataFrame(ls)
    df.to_sql("map_u",Engine,if_exists="append",index=False)
def top_transact():
    path_1 = "C:/Users/Admin/pulse/data/top/transaction/country/india/state/"
    top_trans_state_list = os.listdir(path_1)
    top_trans_state_list
    ls=[]
    for i in top_trans_state_list:
        p=path_1+i+"/"
        top_year=os.listdir(p)
        for j in top_year:
            p_y=p+j+"/"
            files=os.listdir(p_y)
            for file in files:
                with open(p_y+file) as json_file:
                    data=json.load(json_file)
                    for k in range(len(data["data"]["districts"])):
                        final=dict(district=data["data"]["districts"][k]["entityName"],
                                   Year=int(j),
                                   state=i,
                                   count=int(data["data"]["districts"][k]["metric"]["count"]),
                                   amount="{:.2f}".format(data["data"]["districts"][k]["metric"]["amount"]),
                                   quarter=int(file.strip('.json')))
                        ls.append(final)

    df=pd.DataFrame(ls)
    df["amount"].astype(float)
    df.to_sql("top_t",Engine,if_exists="append",index=False)
def top_user():
    path_1 = "C:/Users/Admin/pulse/data/top/user/country/india/state/"
    top_user_state_list = os.listdir(path_1)
    top_user_state_list
    ls=[]
    for i in top_user_state_list:
        p=path_1+i+"/"
        top_year=os.listdir(p)
        for j in top_year:
            p_y=p+j+"/"
            files=os.listdir(p_y)
            for file in files:
                with open(p_y+file) as json_file:
                    data=json.load(json_file)
                    for k in range(len(data["data"]["districts"])):
                        final=dict(Year=int(j),
                                   state=i,
                                   district=data["data"]["districts"][k].get("name"),
                                   registeredUsers=int(data["data"]["districts"][k]["registeredUsers"]),
                                   quarter=int(file.strip('.json')))
                        ls.append(final)
                    

    df=pd.DataFrame(ls)
    df.to_sql("top_u",Engine,if_exists="append",index=False)
    return df

finalfun()


