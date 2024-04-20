# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json
import os
import sqlite3
from itemadapter import ItemAdapter
import mysql.connector
import numpy as np
import psycopg2
from google.oauth2.service_account import Credentials
import gspread
import scrapy
import pandas as pd
import csv
import boto3


class AustinHomeScraperPipeline:
    def process_item(self, item, spider):
        
        adapter = ItemAdapter(item)
        fieldnames = adapter.field_names()

        for fieldname in fieldnames:
            if fieldname == "address":
                value = adapter.get(fieldname)
                temp_val = []
                for val in value:
                    if val.strip() != "":
                        temp_val.append(val.strip())
                adapter[fieldname] = " ".join(temp_val)

                
        return item
    
class SaveToMysqlPipeline:
    #password=os.environ.get('PASSWORD')

    def __init__(self):
        with open("credential.json","r") as file:
            data = json.load(file)
            global db_password
            db_password = data["PASSWORD"]
        self.conn = mysql.connector.connect(host='localhost',
                                user='root',
                                database='austin_home_db',
                                password = db_password
                                
                                )
        
        self.cur = self.conn.cursor()

        create_table_query = """CREATE TABLE IF NOT EXISTS Austin_Homes(
        id INT AUTO_INCREMENT PRIMARY KEY,
        beds TEXT,
        bath TEXT,
        land_size TEXT,
        pricepersqft TEXT,
        agent_name VARCHAR(250),
        agent_number TEXT,
        address VARCHAR(250),
        price TEXT,
        url TEXT
        
        );"""

        self.cur.execute(create_table_query)


    def process_item(self,item,spider):

        insert_query = """ INSERT INTO Austin_Homes
            (beds,bath,land_size,pricepersqft,agent_name,agent_number, address,
            price,url)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """

        self.cur.execute(insert_query,
                         (item['beds'],item['bath'],item['land_size'],
                          item['pricepersqft'],item['agent_name'],item['agent_number'],item['address'],
                          item['price'],item['url']))

        self.conn.commit()
        return item

    def close_spider(self,spider):
        self.cur.close()

class SaveToPostgresqlPipeline:

    def __init__(self):
        
        self.conn = psycopg2.connect(dbname='austin_home_db', user='postgres',
                                     password = db_password)

        self.cur = self.conn.cursor()

        create_table_str =  """CREATE TABLE IF NOT EXISTS Austin_Homes (
                id SERIAL PRIMARY KEY,
                beds TEXT,
                bath TEXT,
                land_size TEXT,
                pricepersqft TEXT,
                agent_name VARCHAR(250),
                agent_number TEXT,
                address VARCHAR(250),
                price TEXT,
                url TEXT
                
            );"""

        self.cur.execute(create_table_str)

    def process_item(self,item,spider):
        
        insert_str = """INSERT INTO Austin_Homes (beds, bath, land_size, pricepersqft, agent_name, agent_number, address, price,url)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        
        self.cur.execute(insert_str,
                         (item['beds'],item['bath'],item['land_size'],
                          item['pricepersqft'],item['agent_name'],item['agent_number'],item['address'],
                          item['price'],item['url']))

        self.conn.commit()

        return item
    
    def close_spider(self,spider):
        self.cur.close()


class SaveToSqlitePipeline:

    def __init__(self):
        self.conn = sqlite3.connect("austin_homes.db")
        self.cur = self.conn.cursor()

        table_str = """CREATE TABLE IF NOT EXISTS Austin_Homes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                beds TEXT,
                bath TEXT,
                land_size TEXT,
                pricepersqft TEXT,
                agent_name VARCHAR(250),
                agent_number TEXT,
                address VARCHAR(250),
                price TEXT,
                url TEXT
                
            );"""
        
        self.cur.execute(table_str)

    
    def process_item(self,item,spider):
        
        insert_str ="""INSERT INTO Austin_Homes (beds, bath, land_size, pricepersqft, agent_name, agent_number, address, price,url)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?,?);"""
        
        self.cur.execute(insert_str,
                         (item['beds'],item['bath'],item['land_size'],
                          item['pricepersqft'],item['agent_name'],item['agent_number'],item['address'],
                          item['price'],item['url']))

        self.conn.commit()

        return item
    
    def close_spider(self,spider):
        self.cur.close()

class GoogleSheetPipeline(object):
    
    def __init__(self):
        # r"C:\Users\User\Desktop\portfolio_projects\home_austin_scrapy\austin_home_scraper\austin-home-data-7d6cc847a642.json"
        #current_dir = os.path.dirname(__file__)
        # file_path = os.path.join(current_dir, "austin-home-data-7d6cc847a642.json")
        self.credentials_file = r"C:\Users\User\Desktop\portfolio_projects\home_austin_scrapy\austin_home_scraper\austin-home-data-7d6cc847a642.json"

        spread_sheet_link = "https://docs.google.com/spreadsheets/d/15yp_qHXAOLmcGJS4omFi_10GgxqJaWu-v_c3xvwwDlo/edit#gid=0"
        self.spread_sheet_id = spread_sheet_link.split("/")[-2]
        self.spread_sheet_name = 'Sheet1'
        self.service = None
        
        
        # self.credentials = Credentials.from_service_account_file(self.credentials_file,scop)
        # gc = gspread.client.Client(auth=credentials)

    def open_sheet(self):
        scope = ['https://spreadsheets.google.com/feeds',
           'https://www.googleapis.com/auth/drive']
        credentials = Credentials.from_service_account_file(self.credentials_file,scopes=scope)
        self.service = gspread.client.Client(auth=credentials)

    def process_item(self,item,spider):

        self.open_sheet()
        #data = pd.DataFrame([item])
        self.write_to_sheet(item)

        return item
    

    def write_to_sheet(self,data):
        

        #data_list = data.values.to_list()
        sheet = self.service.open_by_key(self.spread_sheet_id).worksheet(self.spread_sheet_name)
        
        existing_data = sheet.get_all_values()
        
        has_header = len(existing_data) > 0 and existing_data[0] != []  # Check if first row has values

        if has_header == False:
                    sheet.insert_row(list(data.keys()),1)

        sheet.append_row(list(data.values()))  # Update header only if it's missing

        

        
        
