import json
import pandas as pd
import os
import pymysql
from sqlalchemy import create_engine 
from tabulate import tabulate
import streamlit as st
from PIL import Image
import plotly.express as px

#MySQlConnection
import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Moni1234", 
)

#print(mydb)
mycursor = mydb.cursor(buffered=True)
Engine=create_engine("mysql+pymysql://root:Moni1234@localhost/phonepe")
st.set_page_config(layout="wide")
tab1, tab2, tab3 = st.tabs(["Explore Data", "Insights", "About"])
with tab1:
    mycursor.execute("Use Phonepe")
    col1,col2,col3=st.columns(3)
    selectbox=col1.selectbox("Type",("Transactions", "Users"))
    year_selectbox=col2.selectbox("2018-2023",("2018", "2019","2020","2021","2022","2023"))
    quarter_selectbox=col3.selectbox("Quarter",("1", "2","3","4"))
    a=int(year_selectbox)
    b=int(quarter_selectbox) 
    if(selectbox=="Transactions"):
        mycursor.execute(f"select sum(count) as Transactions , round(sum(amount)) as Value, round((sum(amount)/sum(count))) as Average from agg_t where year= {a} and quarter={b}")
        out=mycursor.fetchall()
        st.success("Transactions value :{:.2f}".format(out[0][0]))
        st.info("Payment value :{:.2f}".format(out[0][1]))
        st.success("Average Transactions value :{:.2f}".format(out[0][2]))
        col1,col2=st.columns(2)
        mycursor.execute(f"select state, sum(amount) from agg_t where year={a} and quarter={b} group by state")
        out=mycursor.fetchall()
        af=pd.DataFrame(out,columns=["state","Transactions"])
        af["state"]=af["state"].map({"andaman-&-nicobar-islands":"Andaman & Nicobar",
                                       "andhra-pradesh":"Andhra Pradesh",
                                       "arunachal-pradesh":"Arunachal Pradesh",
                                       "assam":"Assam",
                                       "bihar":"Bihar",
                                       "chandigarh":"Chandigarh",
                                       "chhattisgarh":"Chhattisgarh",
                                       "dadra-&-nagar-haveli-&-daman-&-diu":"Dadra and Nagar Haveli and Daman and Diu",
                                       "delhi":"Delhi",
                                       "goa":"Goa",
                                       "gujarat":"Gujarat",
                                       "haryana":"Haryana",
                                       "himachal-pradesh":"Himachal Pradesh",
                                       "jammu-&-kashmir":"Jammu & Kashmir",
                                       "jharkhand":"Jharkhand",
                                       "karnataka":"Karnataka",
                                       "kerala":"Kerala",
                                       "ladakh":"Ladakh",
                                       "lakshadweep":"Lakshadweep",
                                       "madhya-pradesh":"Madhya Pradesh",
                                       "maharashtra":"Maharashtra",
                                       "manipur":"Manipur",
                                       "meghalaya":"Meghalaya",
                                       "mizoram":"Mizoram",
                                       "nagaland":"Nagaland",
                                       "odisha":"Odisha",
                                       "puducherry":"Puducherry",
                                       "punjab":"Punjab",
                                       "rajasthan":"Rajasthan",
                                       "sikkim":"Sikkim",
                                       "tamil-nadu":"Tamil Nadu",
                                       "telangana":"Telangana",
                                       "tripura":"Tripura",
                                       "uttar-pradesh":"Uttar Pradesh",
                                       "uttarakhand":"Uttarakhand",
                                       "west-bengal":"West Bengal",})
        fig = px.choropleth(
            af,
geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='state',
            color='Transactions',
            color_continuous_scale='Blues'
        )
        fig.update_geos(fitbounds="locations", visible=False)
        col1.plotly_chart(fig)
        mycursor.execute(f"select  paymenttype, round(sum(amount)) from agg_t where year={a} and quarter={b} group by paymenttype")
        out=mycursor.fetchall()
        df=pd.DataFrame(out,columns=["a","b"])
        fig1=px.pie(values=df["b"],names=df["a"],color_discrete_sequence=px.colors.sequential.RdBu,title="Paymenttype of Selected Year&Quarter")
        fig1.update_traces(textposition='inside', textinfo='percent+label')
        col2.plotly_chart(fig1)
    elif(selectbox=="Users"):
        mycursor.execute(f"select sum(registeredUsers) as registeredUsers,sum(appOpens) as appOpens from agg_u where  year={a} and quarter={b}")
        out=mycursor.fetchall()
        st.success("Registeres Users :{:.2f}".format(out[0][0]))
        st.info("AppOpens :{:.2f}".format(out[0][1]))
        col1,col2=st.columns(2)
        mycursor.execute(f"select district_name, state, registeredUser as ru ,state, appOpens as ao from map_u where  year={a} and quarter= {b} order by registeredUser desc limit 10")
        out=mycursor.fetchall()
        df=pd.DataFrame(out,columns=["district","b","registeredUser","state","ao"])
        fig1=px.bar(df,x="district",y="registeredUser",color="registeredUser", title ="Top 10 states users wise")
        col2.plotly_chart(fig1)
        mycursor.execute(f"select state, sum(registeredusers) from agg_u where year={a} and quarter={b} group by state")
        out=mycursor.fetchall()
        af=pd.DataFrame(out,columns=["state","Users"])
        af["state"]=af["state"].map({"andaman-&-nicobar-islands":"Andaman & Nicobar",
                                       "andhra-pradesh":"Andhra Pradesh",
                                       "arunachal-pradesh":"Arunachal Pradesh",
                                       "assam":"Assam",
                                       "bihar":"Bihar",
                                       "chandigarh":"Chandigarh",
                                       "chhattisgarh":"Chhattisgarh",
                                       "dadra-&-nagar-haveli-&-daman-&-diu":"Dadra and Nagar Haveli and Daman and Diu",
                                       "delhi":"Delhi",
                                       "goa":"Goa",
                                       "gujarat":"Gujarat",
                                       "haryana":"Haryana",
                                       "himachal-pradesh":"Himachal Pradesh",
                                       "jammu-&-kashmir":"Jammu & Kashmir",
                                       "jharkhand":"Jharkhand",
                                       "karnataka":"Karnataka",
                                       "kerala":"Kerala",
                                       "ladakh":"Ladakh",
                                       "lakshadweep":"Lakshadweep",
                                       "madhya-pradesh":"Madhya Pradesh",
                                       "maharashtra":"Maharashtra",
                                       "manipur":"Manipur",
                                       "meghalaya":"Meghalaya",
                                       "mizoram":"Mizoram",
                                       "nagaland":"Nagaland",
                                       "odisha":"Odisha",
                                       "puducherry":"Puducherry",
                                       "punjab":"Punjab",
                                       "rajasthan":"Rajasthan",
                                       "sikkim":"Sikkim",
                                       "tamil-nadu":"Tamil Nadu",
                                       "telangana":"Telangana",
                                       "tripura":"Tripura",
                                       "uttar-pradesh":"Uttar Pradesh",
                                       "uttarakhand":"Uttarakhand",
                                       "west-bengal":"West Bengal",})
        fig = px.choropleth(
            af, geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='state',
            color='Users',
            color_continuous_scale='Blues'
        )
        fig.update_geos(fitbounds="locations", visible=False)
        col1.plotly_chart(fig)
        mycursor.execute(f"select district_name, state, registeredUser as ru ,state, appOpens as ao from map_u where  year={a} and quarter= {b} order by appOpens desc limit 10")
        out=mycursor.fetchall()
        df=pd.DataFrame(out,columns=["district","b","registeredUser","state","ao"])
        fig2=px.bar(df,x="district",y="ao",color="ao",title ="Top 10 states Apps usage wise",)
        st.plotly_chart(fig2)
with tab2:
    mycursor.execute("use phonepe")
    st.subheader("Some intersting facts about Phonepe")
    questions=st.selectbox("Questions: ", ["Please select one",
                                       "The year which has the most no of Transactions?",
                                       "The most prominent paymenttype of Phonepe across years",
                                       "A district who loves the phonepe app the most",
                                       "An effective payment method during the Covid-19 Lockdown period(2019-2020)",
                                       "The Quarter which tops the transaction list very often across years",
                                       "The Quarter which tops the transaction value list very often across years",
                                       "The State which has most the PhonePe Registered users All time",
                                       "The year which recorded most no of Appopens across India",
                                       "The year which recorded highest no of Registered users across India",
                                       "The States which were unaware about Phonepe"])
    if (questions=="Please select one"):
        st.text("Please Choose any one Query")
    elif(questions=="The year which has the most no of Transactions?"):
        mycursor.execute("select year, sum(count) from agg_t group by year")
        out=mycursor.fetchall()
        df=pd.DataFrame(out,columns=["Year","Transact"])
        df["Year"]=df["Year"].astype(str)
        fig=px.bar(df,x="Year",y="Transact",color=df["Transact"],labels={"Transact":"Transactions"})
        st.plotly_chart(fig)
        st.success("2023 has the most no of Transactions so far")
    elif(questions=="The most prominent paymenttype of Phonepe across years"):
        mycursor.execute("select paymenttype, sum(amount) from agg_t group by paymenttype")
        out=mycursor.fetchall()
        df=pd.DataFrame(out,columns=["Paymenttype","Transactions"])
        fig=px.bar(df,x="Paymenttype",y="Transactions",color=df["Transactions"])
        st.plotly_chart(fig)
        st.success("Peer to Peer payments was the most prominent Paymenttype across people over Years")
    elif(questions=="A district who loves the phonepe app the most"):
        mycursor.execute("select district_name, sum(amount) from map_t group by district_name order by sum(amount) desc limit 5")
        out=mycursor.fetchall()
        df=pd.DataFrame(out,columns=["districtname","Transactions"])
        fig=px.bar(df,x="districtname",y="Transactions",color=df["Transactions"])
        st.plotly_chart(fig)
        st.success("bengularu Urban made the most use of PhonePe very often")
    elif(questions=="An effective payment method during the Covid-19 Lockdown period(2019-2020)"):
        mycursor.execute("select paymenttype, sum(amount) from agg_t where year between 2019 and 2021 group by paymenttype order by sum(amount) desc limit 5")
        out=mycursor.fetchall()
        df=pd.DataFrame(out,columns=["paymenttype","Transactions"])
        fig=px.bar(df,x="paymenttype",y="Transactions",color=df["Transactions"])
        st.plotly_chart(fig)
        st.success("Peer to Peer payments was the most prominent Paymenttype during Covid-19")
    elif(questions=="The Quarter which tops the transaction list very often across years"):
        mycursor.execute("select quarter, sum(count) from agg_t group by quarter")
        out=mycursor.fetchall()
        df=pd.DataFrame(out,columns=["quarter","Transactions"])
        df["quarter"]=df["quarter"].astype(str)
        fig=px.bar(df,x="quarter",y="Transactions",color=df["Transactions"])
        st.plotly_chart(fig)
        st.success("Third Quarter Tops the Chart with far margin")
    elif(questions=="The Quarter which tops the transaction value list very often across years"):
        mycursor.execute("select quarter, sum(amount) from agg_t group by quarter")
        out=mycursor.fetchall()
        df=pd.DataFrame(out,columns=["quarter","Transactions"])
        fig=px.bar(df,x="quarter",y="Transactions",color=df["Transactions"])
        df["quarter"]=df["quarter"].astype(str)
        st.plotly_chart(fig)
        st.success("Third Quarter Tops the Chart with far margin")
    elif(questions=="The State which has most the PhonePe Registered users All time"):
        mycursor.execute("select state, sum(registeredusers) from top_u group by state order by sum(registeredusers) desc limit 5")
        out=mycursor.fetchall()
        df=pd.DataFrame(out,columns=["state","Transactions"])
        fig=px.bar(df,x="state",y="Transactions",color=df["Transactions"])
        st.plotly_chart(fig)
        st.success("Maharastra has more phonepe users than other state in INDIA.")
    elif(questions=="The year which recorded most no of Appopens across India"):
        mycursor.execute("select year, sum(appopens) from map_u group by year")
        out=mycursor.fetchall()
        df=pd.DataFrame(out,columns=["year","Transactions"])
        df["year"]=df["year"].astype(str)
        fig=px.bar(df,x="year",y="Transactions",color=df["Transactions"])
        st.plotly_chart(fig)
        st.success("Current year(2023) wins the chart with even a quarter less to its tally")
    elif(questions=="The year which recorded highest no of Registered users across India"):
        mycursor.execute("select year, sum(registereduser) from map_u group by year")
        out=mycursor.fetchall()
        df=pd.DataFrame(out,columns=["year","Transactions"])
        df["year"]=df["year"].astype(str)
        fig=px.bar(df,x="year",y="Transactions",color=df["Transactions"])
        st.plotly_chart(fig)
        st.success("Year(2022) had more success among other years")
    elif(questions=="The States which were unaware about Phonepe"):
        mycursor.execute("select state, sum(appopens) from map_u group by state order by sum(appopens) limit 5")
        out=mycursor.fetchall()
        df=pd.DataFrame(out,columns=["state","Transactions"])
        fig=px.bar(df,x="state",y="Transactions",color=df["Transactions"])
        st.plotly_chart(fig)
        st.success("Andaman and Lakshadeep island are unfamiliar about Phonepe in INDIA.")
    col1,col2=st.columns(2)
    with col1:
        mycursor.execute("select year,sum(registeredUsers),sum(appOpens) from agg_u group by year")
        out=mycursor.fetchall()
        df=pd.DataFrame(out, columns=["year","registeredUsers","appOpens"])
        fig=px.line(df,x="year",y="registeredUsers",title="Phonepe Users Growth Over the Years")
        st.plotly_chart(fig)
    fig1=px.line(df,x="year",y="appOpens",title="Phonepe Appopens Growth Over the Years")
    col2.plotly_chart(fig1)
with tab3:
     with st.container():        
        st.header("Phonepe Pulse Data Visualization and Exploration: A User-Friendly Tool Using Streamlit and Plotly")
        st.image(Image.open(r"C:\Users\Admin\Desktop\PhonePe\image8.png"),width=700)
        st.subheader("PhonePe Pulse is your window to the world of how India transacts with interesting trends, deep insights and in-depth analysis based on our data put together by the PhonePe team. A live geo visualization dashboard that displays information and insights from the Phonepe pulse and will be able to access the dashboard from a web browser and easily navigate the different visualizations and facts and figures displayed.")
        
    
    
    
        

    