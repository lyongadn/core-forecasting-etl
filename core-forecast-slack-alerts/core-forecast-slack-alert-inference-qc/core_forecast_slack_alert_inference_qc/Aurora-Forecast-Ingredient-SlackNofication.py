# coding: utf-8

# In[ ]:


import boto3
import json
import logging
logger = logging.getLogger(__name__)
import os
from base64 import b64decode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
# Packages needed
#Data Preprocessing packages
import pandas as pd
import numpy as np
from boto3.s3.transfer import S3Transfer
from io import StringIO
import os.path
import csv
import sys
import re
#Calendar / Time computation packages
import time
import datetime
from pytz import timezone
from datetime import date,datetime,time,timedelta
from pytz import timezone
import pymysql
pymysql.install_as_MySQLdb()

def send_slack_email(message):
     # The base-64 encoded, encrypted key (CiphertextBlob) stored in the kmsEncryptedHookUrl environment variable
    ENCRYPTED_HOOK_URL = os.environ['ENCRYPTED_HOOK_URL_NAME']
    # The Slack channel to send a message to stored in the slackChannel environment variable
    SLACK_CHANNEL = "prod-alerts"
    HOOK_URL = os.environ['HOOK_URL_LINK']
    #logger.info("Message: " + str(message))
    slack_message = {
        'channel': SLACK_CHANNEL,
        'text': message,
        "username": "Forecast-Ingredient-15min-QC",
        "icon_emoji": ":chicken:"
    }
    req = Request(HOOK_URL, json.dumps(slack_message).encode('utf-8'))
    try:
        response = urlopen(req)
        response.read()
        #logger.info("Message posted to %s", slack_message['channel'])
    except HTTPError as e:
        logger.error("Request failed: %d %s", e.code, e.reason)
    except URLError as e:
        logger.error("Server connection failed: %s", e.reason)
connection_rep1 = pymysql.connect(host= os.environ['host_link'],
                                      user= os.environ['user_name'],
                                      password= os.environ['rds_password']
                                      )
print("Connection of Replica suceeded")


# In[ ]:


##################################################################
############ Tables needed for Ingredientlevel QC ################
########### 1. Lookup Ingredient table            ################
########### 2. YCF table                          ################
########### 3.15min_ingredients_lstm_10days_ahead ################
########### 4.15min_ingredients_lstm_30days_ahead ################
##################################################################

def lambda_handler(event, context):
    with connection_rep1.cursor() as rep1:

    ###########  Lookup Ingredient table            ################    
            lookup_ingredients = '''select * from ml_preprod.lookup_ingredients
            where generation_date = (select max(generation_date) from ml_preprod.lookup_ingredients)'''
            rep1.execute(lookup_ingredients)
            lookup_ingredients_data = rep1.fetchall()
            lookup_ingredients_data= pd.DataFrame(list(lookup_ingredients_data), columns=['pin','recipe_item_id','recipe_name',
                'ingredient_name','ingredient_id','unit_of_measure','ingredient_quantity_actual','ingredient_quantity','generation_date'])
                
    ###########  YCF table                          ################        
            ycf_ingredients_monthly = '''select * from ml_preprod.ycf_ingredients_monthly 
            where generation_date = (select max(generation_date) from ml_preprod.ycf_ingredients_monthly);'''
            rep1.execute(ycf_ingredients_monthly)
            ycf_ingredients_monthly_data = rep1.fetchall()
            ycf_ingredients_monthly_data = pd.DataFrame(list(ycf_ingredients_monthly_data), columns=['location_num','ingredient_id','ycf','generation_date'])

    ########### ingredients_lstm_10days_ahead data at daily level ################   

            ingredients_lstm_10days_ahead_data = '''select location_num,business_date,ingredient_id,sum(forecast) ing_forecast,count(*) No_of_rows,generation_date from ui_preprod.15min_ingredients_lstm_10days_ahead
            where generation_date = (select max(generation_date) from ui_preprod.15min_ingredients_lstm_10days_ahead)
            group by location_num,business_date,ingredient_id,generation_date
            order by location_num,business_date,ingredient_id,generation_date'''
            rep1.execute(ingredients_lstm_10days_ahead_data)
            ingredients_lstm_10days_ahead_data = rep1.fetchall()
            ingredients_lstm_10days_ahead_data = pd.DataFrame(list(ingredients_lstm_10days_ahead_data),columns=['location_num','business_date','ingredient_id','forecast','No_of_rows','generation_date'])

    ########### ingredients_lstm_30days_ahead data at daily level  ################

            ingredients_lstm_30days_ahead_data = '''select location_num,business_date,ingredient_id,sum(forecast) ing_forecast,count(*) No_of_rows,generation_date from ui_preprod.15min_ingredients_lstm_30days_ahead
                    where generation_date = (select max(generation_date) from ui_preprod.15min_ingredients_lstm_30days_ahead)
                    group by location_num,business_date,ingredient_id,generation_date
                    order by location_num,business_date,ingredient_id,generation_date'''
            rep1.execute(ingredients_lstm_30days_ahead_data)
            ingredients_lstm_30days_ahead_data = rep1.fetchall()
            ingredients_lstm_30days_ahead_data = pd.DataFrame(list(ingredients_lstm_30days_ahead_data),columns=['location_num','business_date','ingredient_id','forecast','No_of_rows','generation_date'])


    ########### ingredients_lstm_10days_ahead interval timings  ################
            ingredients_lstm_10days_interval = '''select interval_start_time,count(*) from ui_preprod.15min_ingredients_lstm_10days_ahead 
            where generation_date = (select max(generation_date) from ui_preprod.15min_ingredients_lstm_10days_ahead)
            GROUP BY interval_start_time
            ORDER BY interval_start_time'''
            rep1.execute(ingredients_lstm_10days_interval)
            ingredients_lstm_10days_interval = rep1.fetchall()
            ingredients_lstm_10days_interval = pd.DataFrame(list(ingredients_lstm_10days_interval),columns=['interval_start_time','no_of_rows'])

    ########### ingredients_lstm_30days_ahead interval timings  ################
            ingredients_lstm_30days_interval = '''select interval_start_time,count(*) from ui_preprod.15min_ingredients_lstm_10days_ahead 
            where generation_date = (select max(generation_date) from ui_preprod.15min_ingredients_lstm_10days_ahead)
            GROUP BY interval_start_time
            ORDER BY interval_start_time'''
            rep1.execute(ingredients_lstm_30days_interval)
            ingredients_lstm_30days_interval = rep1.fetchall()
            ingredients_lstm_30days_interval = pd.DataFrame(list(ingredients_lstm_30days_interval),columns=['interval_start_time','no_of_rows'])

    list_15min = [datetime.combine(date.today(), time.min)  + timedelta(minutes=15*x) for x in range(0, 96)]
    interval_15min = [x.strftime('%H:%M:%S') for x in list_15min]
    interval_15min = pd.DataFrame(interval_15min,columns=['expected_intervals'])
    interval_15min['expected_intervals'] = pd.to_timedelta(interval_15min['expected_intervals'])
    interval_10days= interval_15min.merge(ingredients_lstm_10days_interval,left_on='expected_intervals', right_on='interval_start_time')
    interval_30days= interval_15min.merge(ingredients_lstm_30days_interval,left_on='expected_intervals', right_on='interval_start_time')
    temp_var = ""
    Flag = 0
    if not (len(ycf_ingredients_monthly_data) == len(ycf_ingredients_monthly_data[['location_num','ingredient_id']].drop_duplicates())):
        temp_var += "\n"
        temp_var += "location_num + ingredient is not unique in ycf ingredients monthly"
        Flag = 1
    if not (len(lookup_ingredients_data) == len(lookup_ingredients_data.drop_duplicates())):
        temp_var += "\n"
        temp_var += "pin + ingredient is not unique in lookup ingredients data"
        Flag = 1
    if not (len(interval_10days) == 96):
        temp_var += "\n"
        temp_var += "unexpected number of intervals at 10days ahead forecast "
        Flag = 1

    if not (len(interval_30days) == 96):
        temp_var += "\n"
        temp_var += "unexpected number of intervals at 30days ahead forecast "
        Flag = 1

    if not (len(ingredients_lstm_10days_ahead_data[ingredients_lstm_10days_ahead_data['No_of_rows'] != 96]) == 0):

        temp_var += "\n"
        temp_var += "unexpected number of rows at 10days ahead forecast"
        Flag = 1

    if not (len(ingredients_lstm_30days_ahead_data[ingredients_lstm_30days_ahead_data['No_of_rows'] != 96]) == 0):
        temp_var += "\n"
        temp_var += "unexpected number of rows at 10days ahead forecast"
        Flag = 1

    if (temp_var == ""):
        temp_var += "Ingredient forecast is successful"
    
        
    ####################### Based on the Flag variable status update the s3 with the file which triggers the next pipeline ################
    #if Flag == 1:
     #   uploadToS3("Failed")
      #  Flag = 0
    #else:
     #   uploadToS3("Success")
      #  Flag = 0     
    send_slack_email(temp_var)
  
    return()