import os
import json
import pandas as pd
from pprint import pprint
import psycopg2
import mysql.connector
import pymysql
from PIL import Image
import plotly.express as px



path_1 = "D:/DS/Capstone/CP_02-Phonepe Pulse Data Visualization and Exploration/CP_02_project/Git_hub_repo_clone/pulse/data/aggregated/transaction/country/india/state/"

path_2 = "D:/DS/Capstone/CP_02-Phonepe Pulse Data Visualization and Exploration/CP_02_project/Git_hub_repo_clone/pulse/data/aggregated/user/country/india/state/"

path_3 = "D:/DS/Capstone/CP_02-Phonepe Pulse Data Visualization and Exploration/CP_02_project/Git_hub_repo_clone/pulse/data/map/transaction/hover/country/india/state/"

path_4 = "D:/DS/Capstone/CP_02-Phonepe Pulse Data Visualization and Exploration/CP_02_project/Git_hub_repo_clone/pulse/data/map/user/hover/country/india/state/"

path_5 = "D:/DS/Capstone/CP_02-Phonepe Pulse Data Visualization and Exploration/CP_02_project/Git_hub_repo_clone/pulse/data/top/transaction/country/india/state/"

path_6 = "D:/DS/Capstone/CP_02-Phonepe Pulse Data Visualization and Exploration/CP_02_project/Git_hub_repo_clone/pulse/data/top/user/country/india/state/"



#Aggregat Transaction

path_1 = "D:/DS/Capstone/CP_02-Phonepe Pulse Data Visualization and Exploration/CP_02_project/Git_hub_repo_clone/pulse/data/aggregated/transaction/country/india/state/"

columns_1 = {"states":[], "years":[], "quarter":[], "transaction_type":[], "transaction_count":[], "transaction_amount":[] }

agg_tran_list = os.listdir(path_1)

for state in agg_tran_list:
    cur_state = path_1 + state + "/"
    agg_year_list = os.listdir(cur_state)
    
    for year in agg_year_list:
        cur_year = cur_state + year +"/"
        agg_file_list = os.listdir(cur_year)

        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")

            A = json.load(data)

            for i in A["data"]["transactionData"]:
                name = i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]

                columns_1["states"].append(state)
                columns_1["years"].append(year)
                columns_1["quarter"].append(int(file.strip(".json")))
                columns_1["transaction_type"].append(name)
                columns_1["transaction_count"].append(count)
                columns_1["transaction_amount"].append(amount)

Agg_Transaction = pd.DataFrame(columns_1)
        


#Aggregat User

path_2 = "D:/DS/Capstone/CP_02-Phonepe Pulse Data Visualization and Exploration/CP_02_project/Git_hub_repo_clone/pulse/data/aggregated/user/country/india/state/"

columns_2 = {"states":[], "years":[], "quarter":[], "brands":[], "user_count":[], "user_percentage":[] }

agg_user_list = os.listdir(path_2)

for state in agg_user_list:
    cur_state = path_2 + state + "/"
    agg_year_list = os.listdir(cur_state)
    
    for year in agg_year_list:
        cur_year = cur_state + year +"/"
        agg_file_list = os.listdir(cur_year)

        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")

            B = json.load(data)
            
            try:
                for i in B["data"]["usersByDevice"]:
                    brand = i["brand"]
                    count = i["count"]
                    percentage = i["percentage"]
                    columns_2["states"].append(state)
                    columns_2["years"].append(year)
                    columns_2["quarter"].append(int(file.strip(".json")))
                    columns_2["brands"].append(brand)
                    columns_2["user_count"].append(count)
                    columns_2["user_percentage"].append(percentage)
            except:
                pass

agg_user = pd.DataFrame(columns_2)



#Map Transaction

path_3 = "D:/DS/Capstone/CP_02-Phonepe Pulse Data Visualization and Exploration/CP_02_project/Git_hub_repo_clone/pulse/data/map/transaction/hover/country/india/state/"

columns_3 = {"states":[], "years":[], "quarter":[], "districts":[], "transaction_count":[], "transaction_amount":[] }

Map_tran_list = os.listdir(path_3)

for state in Map_tran_list:
    cur_state = path_3 + state + "/"
    Map_year_list = os.listdir(cur_state)
    
    for year in agg_year_list:
        cur_year = cur_state + year +"/"
        Map_file_list = os.listdir(cur_year)

        for file in Map_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")

            C = json.load(data)

            for i in C["data"]["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]

                columns_3["states"].append(state)
                columns_3["years"].append(year)
                columns_3["quarter"].append(int(file.strip(".json")))
                columns_3["districts"].append(name)
                columns_3["transaction_count"].append(count)
                columns_3["transaction_amount"].append(amount)     

Map_Transaction = pd.DataFrame(columns_3)
        


#Map Users

path_4 = "D:/DS/Capstone/CP_02-Phonepe Pulse Data Visualization and Exploration/CP_02_project/Git_hub_repo_clone/pulse/data/map/user/hover/country/india/state/"

columns_4 = {"states":[], "years":[], "quarter":[], "districts":[], "registeredUsers":[], "appOpens":[] }

Map_user_list = os.listdir(path_4)

for state in Map_user_list:
    cur_state = path_4 + state + "/"
    Map_year_list = os.listdir(cur_state)
    
    for year in agg_year_list:
        cur_year = cur_state + year +"/"
        Map_file_list = os.listdir(cur_year)

        for file in Map_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")

            D = json.load(data)
            
            for i in D["data"]["hoverData"].items():
                district = i[0]
                registeredUsers = i[1]["registeredUsers"]
                appOpens = i[1]["appOpens"]

                columns_4["states"].append(state)
                columns_4["years"].append(year)
                columns_4["quarter"].append(int(file.strip(".json")))
                columns_4["districts"].append(district)
                columns_4["registeredUsers"].append(registeredUsers)
                columns_4["appOpens"].append(appOpens)
                
Map_users = pd.DataFrame(columns_4)



#Top Transaction

path_5 = "D:/DS/Capstone/CP_02-Phonepe Pulse Data Visualization and Exploration/CP_02_project/Git_hub_repo_clone/pulse/data/top/transaction/country/india/state/"

columns_5 = {"states":[], "years":[], "quarter":[], "pincodes":[], "transaction_count":[], "transaction_amount":[] }

top_trans_list = os.listdir(path_5)

for state in top_trans_list:
    cur_state = path_5 + state + "/"
    top_year_list = os.listdir(cur_state)
    
    for year in top_year_list:
        cur_year = cur_state + year +"/"
        top_file_list = os.listdir(cur_year)

        for file in top_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")

            E = json.load(data)
            
            for i in E["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]

                columns_5["states"].append(state)
                columns_5["years"].append(year)
                columns_5["quarter"].append(int(file.strip(".json")))
                columns_5["pincodes"].append(entityName)
                columns_5["transaction_count"].append(count)
                columns_5["transaction_amount"].append(amount)

top_transcation = pd.DataFrame(columns_5)



#Top Users

path_6 = "D:/DS/Capstone/CP_02-Phonepe Pulse Data Visualization and Exploration/CP_02_project/Git_hub_repo_clone/pulse/data/top/user/country/india/state/"

columns_6 = {"states":[], "years":[], "quarter":[], "pincodes":[], "registeredUsers":[]}

top_user_list = os.listdir(path_5)

for state in top_user_list:
    cur_state = path_6 + state + "/"
    top_year_list = os.listdir(cur_state)
    
    for year in top_year_list:
        cur_year = cur_state + year +"/"
        top_file_list = os.listdir(cur_year)

        for file in top_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")

            F = json.load(data)

            for i in F["data"]["pincodes"]:
                name = i["name"]
                registeredUsers = i["registeredUsers"]

                columns_6["states"].append(state)
                columns_6["years"].append(year)
                columns_6["quarter"].append(int(file.strip(".json")))
                columns_6["pincodes"].append(name)
                columns_6["registeredUsers"].append(registeredUsers)
                
top_users = pd.DataFrame(columns_6)



# SQL Connection

with open('mysql_credentials.txt', 'r') as file:
    lines = file.readlines()
    username, Password = lines[0].strip().split(':')

connection = pymysql.connect(host = "localhost",
                        user = username,
                        password = Password
                        )

mycursor = connection.cursor()

mycursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_db")

connection.commit()
# mycursor.close()
# connection.close()



# CREATE TABLE type_of_pay
mycursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_db")
mycursor.execute('USE phonepe_db')

create_query1 = '''CREATE TABLE if not exists type_of_pay (States varchar(50),
                                                                      Years int,
                                                                      Quarter int,
                                                                      Transaction_type varchar(50),
                                                                      Transaction_count bigint,
                                                                      Transaction_amount bigint
                                                                      )'''
mycursor.execute(create_query1)
connection.commit()

for index,row in Agg_Transaction.iterrows():
    insert_query1 = '''INSERT INTO type_of_pay (States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount)
                                                        values(%s,%s,%s,%s,%s,%s)'''
data_to_insert = Agg_Transaction.to_records(index=False).tolist()

# Use executemany to insert multiple rows
mycursor.executemany(insert_query1, data_to_insert)
connection.commit()


# CREATE TABLE type_of_brand
mycursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_db")
mycursor.execute('USE phonepe_db')

# Create the 'type_of_brand' table
create_query1 = '''
CREATE TABLE IF NOT EXISTS type_of_brand (
    States VARCHAR(50),
    Years INT,
    Quarter INT,
    brands VARCHAR(50),
    Transaction_count BIGINT,
    Percentage float
)
'''
mycursor.execute(create_query1)
connection.commit()

# Assuming Agg_Trans is a DataFrame containing your data
insert_info = '''
INSERT INTO type_of_brand (States, Years, Quarter, brands, Transaction_count,Percentage)
VALUES (%s, %s, %s, %s, %s, %s)
'''

# Convert DataFrame to list of tuples for executemany
data_to_insert = agg_user.to_records(index=False).tolist()

# Use executemany to insert multiple rows
mycursor.executemany(insert_info, data_to_insert)
connection.commit()


# CREATE TABLE transaction_count
mycursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_db")
mycursor.execute('USE phonepe_db')
create_query3 = '''CREATE TABLE if not exists transaction_count (States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                District varchar(50),
                                                                Transaction_count bigint,
                                                                Transaction_amount float)'''
mycursor.execute(create_query3)
connection.commit()

# for index,row in map_transaction.iterrows():
insert_query3 = '''
                INSERT INTO Transaction_count (States, Years, Quarter, District, Transaction_count, Transaction_amount)
                VALUES (%s, %s, %s, %s, %s, %s)

            '''
data_to_insert = Map_Transaction.to_records(index=False).tolist()

# Use executemany to insert multiple rows
mycursor.executemany(insert_query3, data_to_insert)
connection.commit()


# CREATE TABLE Register_user
mycursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_db")
mycursor.execute('USE phonepe_db')
create_query4 = '''CREATE TABLE if not exists Register_user (States varchar(50),
                                                        Years int,
                                                        Quarter int,
                                                        Districts varchar(50),
                                                        RegisteredUser bigint,
                                                        AppOpens bigint)'''
mycursor.execute(create_query4)
connection.commit()

# for index,row in map_user.iterrows():
insert_query4 = '''INSERT INTO Register_user (States, Years, Quarter, Districts, RegisteredUser, AppOpens)
                        values(%s,%s,%s,%s,%s,%s)'''
data_to_insert = Map_users.to_records(index=False).tolist()

# Use executemany to insert multiple rows
mycursor.executemany(insert_query4, data_to_insert)
connection.commit()


# CREATE TABLE top_transaction
mycursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_db")
mycursor.execute('USE phonepe_db')
create_query5 = '''CREATE TABLE if not exists top_transaction (States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                pincodes int,
                                                                Transaction_count bigint,
                                                                Transaction_amount bigint)'''
mycursor.execute(create_query5)
connection.commit()

# for index,row in top_transaction.iterrows():
insert_query5 = '''INSERT INTO top_transaction (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
                                                    values(%s,%s,%s,%s,%s,%s)'''
data_to_insert = top_transcation.to_records(index=False).tolist()

# Use executemany to insert multiple rows
mycursor.executemany(insert_query5, data_to_insert)
connection.commit()


# CREATE TABLE top_user

mycursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_db")
mycursor.execute('USE phonepe_db')
create_query6 = '''CREATE TABLE if not exists top_user (States varchar(50),
                                                        Years int,
                                                        Quarter int,
                                                        Pincodes int,
                                                        RegisteredUser bigint
                                                        )'''
mycursor.execute(create_query6)
connection.commit()

for index,row in top_users.iterrows():
    insert_query6 = '''INSERT INTO top_user (States, Years, Quarter, Pincodes, RegisteredUser)
                                            values(%s,%s,%s,%s,%s)'''
data_to_insert = top_users.to_records(index=False).tolist()

# Use executemany to insert multiple rows
mycursor.executemany(insert_query6, data_to_insert)
connection.commit()

mycursor.close()
connection.close()











