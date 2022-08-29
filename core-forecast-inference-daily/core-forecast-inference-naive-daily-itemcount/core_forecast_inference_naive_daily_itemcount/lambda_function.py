'''
This script talks about the ML logic of the naive script used to forecast for items which do not pass the criteria for LSTM forecast
'''

import os.path
import json

import gc
import datetime
import logging

# from datetime import date, timedelta

import pandas as pd
import numpy as np

import boto3
from boto3.s3.transfer import S3Transfer

from pytz import timezone
import pymysql

from configs import Config


logging.basicConfig(format="%(name)s - %(levelname)s - %(message)s")
global add_log, status
add_log = logging.getLogger("__naive__")
add_log.setLevel(20)




def naive_models(data_in, product_num, location_num, conf):
   '''In this function naive logic is defined- forecast value is median of last 30 days sales
   Parameters
   ----------
   - data_in: input dataframe - one product
   - product_num : product num in dataframe
   - location_num : location number
   - conf  - configuration  object
   Returns:
   ----------
   - Three dataframes of shorter, longer and redundancy forecast
   '''
   status = " naive model function "
   data_in['business_date'] = pd.to_datetime(data_in['business_date'])
   data_in['target'] = data_in['sum_daily_quantity']
   data_f = data_in[['business_date', 'target']]
   data_a = pd.to_datetime(
       (data_in['max_date'].max() - datetime.timedelta(days=2)).strftime('%Y-%m-%d'))
   numdays = 30
   date_list = []
   data = pd.DataFrame()
   for i in range(0, numdays):
       date_list.append(data_a - datetime.timedelta(days=i))
   data['business_date'] = date_list
   data['business_date'] = pd.to_datetime(data['business_date'])
   data_f = pd.merge(data_f, data, how='right', on=['business_date'])
   data_f['target'] = np.nan_to_num(data_f['target'])
   data_f = data_f.sort_values('business_date')
   data_f['forecast'] = 0
   for i in range(1, 35):
       forecast = float(data_f[(data_f['business_date'] <= max(data_f['business_date'])) \
                               & (data_f['business_date'] > (data_f['business_date'].max() - \
                                           datetime.timedelta(days=30)))]['target'].median())

       business_date = data_f['business_date'].max() + \
           datetime.timedelta(days=1)
       if forecast < 0:
           target = 0
       else:
           target = forecast
       data_f.loc[data_f.index.max()+i, :] = business_date, target, forecast
   output = data_f[data_f['business_date'] > (data_in['max_date'].max() - \
            datetime.timedelta(days=2)).strftime('%Y-%m-%d')]
   location_num = str(location_num)
   product = int(product_num)
   output['location_num'] = location_num
   output['product'] = product
   output = output[['location_num', 'product', 'business_date', 'forecast']]
   output = output.reset_index()
   output = output.drop('index', axis=1)
   df_ten = output.loc[int(conf.get_shorter_forecast_start()):int( \
                           conf.get_shorter_forecast_end()), :]
   df_thirty = output.loc[int(conf.get_longer_forecast_start()):int( \
                              conf.get_longer_forecast_end()), :]
   df_redundancy = output.loc[int(conf.get_redundancy_forecast_start()):int( \
                              conf.get_redundancy_forecast_end()), :]
   gc.collect()
   return df_ten, df_thirty, df_redundancy


def execute_query(query, conf):
   '''In this function data is fetched from database
   Parameters
   ----------
   - query : SQL query
   - conf : config object
   Returns:
   ----------
   - data : data in list
   '''
   status = " execute query "
   kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
   keys = kms_manager.get_secret_value(SecretId=conf.get_secret_key())
   print("Credentials fetched")
   credentials = json.loads(keys['SecretString'])
   try :

      connection_rep1 = pymysql.connect(host=conf.get_replica_host_link(),
                                 user=credentials['username'],
                                 password=credentials['password'],
                                 db=conf.get_database())
      print("connection Succeded")
      with connection_rep1.cursor() as rep1:
         rep1.execute(query)
         data = rep1.fetchall()
         print (data)
   except Exception as e:
      print(e)
   return list(data)


def get_non_zero_forecast(list_holidays, item_store_old, df_naive):
   '''In this function forecast value is made zero for all the store product combinations
   with no data in last 30days
   Parameters
   ----------
   - list_holidays: holidays business dates
   - item_store_old : dataframe with all product store combinations
   - df_naive : shorter or longer or redundancy forecast datframe
   Returns:
   ----------
   - return_df : dataframe with all products - no data in last 30 days and products with
   naive forecast
   '''
   dataframe = pd.DataFrame()
   dataframe['business_date'] = df_naive['business_date'].unique()
   dataframe['key'] = 1
   item_store_old['key'] = 1

   dataframe = pd.merge(dataframe, item_store_old, on='key')

   dataframe['forecast'] = 0.0
   dataframe = dataframe[['location_num',
                          'product', 'business_date', 'forecast']]

   return_df = df_naive.append(dataframe, ignore_index=True)

   return_df['generation_date'] = (datetime.datetime.now(
       tz=timezone('US/Pacific'))).strftime("%Y-%m-%d")

   return_df.loc[return_df['business_date'].isin(
       list_holidays), 'forecast'] = 0.0

   return return_df


def naive_len_grt_zero(item_store, item_store_naive, today_date, location_num, list_holidays, \
                       conf):
   '''In this function naive model is called for each product and forecast dataframes are uploaded
   for shorter, longer and redundancy business date ranges
   Parameters
   ----------
   - item_store: holidays business dates
   - item_store_naive : dataframe with all product store combinations
   - today_date : today business date
   - location_num :  location number
   - list_holidays : list of holidays - store closed
   - conf : configuration object
   '''
   status = " get naive forecast for products with data in last 30 days "

   location_nums = item_store_naive['location_num'].unique()
   location_nums = [str(x) for x in location_nums]
   location_nums = ",".join(location_nums)

   product_picked = item_store_naive['product'].tolist()
   product_picked = [str(x) for x in product_picked]
   product_picked = ",".join(product_picked)

   sales_data = get_sales_data(conf, location_nums, product_picked)
   sales_data['max_date'] = today_date
   productdata = sales_data.groupby('product', sort=False)

   df_ten_naive = pd.DataFrame()
   df_thirty_naive = pd.DataFrame()
   df_redundancy_naive = pd.DataFrame()
   for name, group in productdata:
       data_x, data_y, data_z = naive_models(group, name, location_num, conf)
       df_ten_naive = df_ten_naive.append(data_x, ignore_index=True)
       df_thirty_naive = df_thirty_naive.append(data_y, ignore_index=True)
       df_redundancy_naive = df_redundancy_naive.append(
           data_z, ignore_index=True)
   item_store_old = item_store[pd.to_datetime(item_store['max_business_date']) < (
       today_date - datetime.timedelta(days=30)).strftime("%Y-%m-%d")]

   add_log.info(status + " -  success")

  # forecasting zero for products with no sales in last 30 days

   status = " get zero forecast for products with no data in last 30 days "
   df_ten = get_non_zero_forecast(list_holidays, item_store_old, df_ten_naive)
   df_thirty = get_non_zero_forecast(
       list_holidays, item_store_old, df_thirty_naive)
   df_redundancy = get_non_zero_forecast(
       list_holidays, item_store_old, df_redundancy_naive)
   # Updloading the final forecast dataframe to S3

   add_log.info(status + " -  success")

   status = " shorter, longer and redundancy forecast uploaded to s3"

   local_dir = '/tmp/'
   upload_dataframe(df_ten, local_dir, 'tendayAhead_Naive.csv', conf.get_prod_bucket(), \
                    conf.get_shorter_forecast_upload_key())
   upload_dataframe(df_thirty, local_dir, 'thirtydayAhead_Naive.csv', \
                    conf.get_prod_bucket(), conf.get_longer_forecast_upload_key())
   upload_dataframe(df_redundancy, local_dir, 'thirtyfourAhead_Naive.csv', conf.get_prod_bucket(),\
                    conf.get_redundancy_forecast_upload_key())

   add_log.info(status + " - success")

def upload_dataframe(dataframe, local_dir, file_name, s_3_bucket, s_3_key):

   '''In this function input datframe is saved in local directory and uploade to s3
   Parameters
   ----------
   - dataframe: datframe to be saved
   - local_dir : local directory path to store csv
   - file_name : filename to save dataframe
   - s_3_bucket : s3 bucket to upload datframe
   - s_3_key : path in S3 bucket
   '''
   status = " upload dataframe to s3 "
   dataframe.to_csv(local_dir+file_name, index=False, header=False)
   s_3 = boto3.client('s3')
   transfer = S3Transfer(s_3)
   transfer.upload_file(os.path.join(local_dir, file_name), s_3_bucket, s_3_key)

def get_business_date_dict(today_date, start, end):

   '''In this function list of business dates is returned
   Parameters
   ----------
   - today_date: reference business date
   - start : nth day start from today_date
   - end : nth_day end from today_date
   Returns:
   ----------
   -  dictionary with key - business_date and value -list of business dates
   '''
   status = " get business dates list for zero forecast "
   dt_list = []
   for i in range(start, end):
       dt_list.append((today_date + datetime.timedelta(days=i)).strftime("%Y-%m-%d"))
   return {'business_date': dt_list}



def get_zero_forecast(today_date, start, end, item_store_old, df_naive):

   '''In this function forecast is made zero for all products in item_store_old dataframe
   for a list of business dates
   Parameters
   ----------
   - today_date: reference business date
   - start : nth day start from today_date
   - end : nth_day end from today_date
   - item_store_old : dataframe with store product combinations sales
   Returns:
   ----------
   -  return_df : dataframe with forecast zero
   '''
   status = " forecast zero for products with no sales in last 30 days "

   bd_list = get_business_date_dict(today_date, start, end)
   dataframe = pd.DataFrame(data=bd_list)
   dataframe['key'] = 1
   item_store_old['key'] = 1

   dataframe_merge = pd.merge(dataframe, item_store_old, on='key')
   dataframe_merge['forecast'] = 0.0
   dataframe_merge = dataframe_merge[['location_num',
                                      'product', 'business_date', 'forecast']]

   return_df = df_naive.append(dataframe_merge, ignore_index=True)

   return_df['generation_date'] = (datetime.datetime.now(
       tz=timezone('US/Pacific'))).strftime("%Y-%m-%d")

   return return_df


def naive_no_data_in_last_thirty_days(item_store, today_date, conf):

   '''This function is called when all products in store_product combinations have no
   sales in last 30 days
   Parameters
   ----------
   - item_store: dataframe with all product itemcount
   - today_date : reference date
   - location_num : location number of store
   - conf : configuration object
   '''

   status = " forecast zero for all products with no data in last 30 days"

   #entered the No data in 30days function
   item_store_old = item_store[pd.to_datetime(item_store['max_business_date']) < (
       today_date - datetime.timedelta(days=30)).strftime("%Y-%m-%d")]

   df_ten_naive = pd.DataFrame()
   df_thirty_naive = pd.DataFrame()
   df_redundancy_naive = pd.DataFrame()
   df_ten = get_zero_forecast(today_date, int(conf.get_shorter_forecast_start()), \
           int(conf.get_shorter_forecast_end()), item_store_old, df_ten_naive)
   df_thirty = get_zero_forecast(today_date, int(conf.get_longer_forecast_start()), \
               int(conf.get_longer_forecast_end()), item_store_old, df_thirty_naive)
   df_redundancy = get_zero_forecast(today_date, int(conf.get_redundancy_forecast_start()), \
                   int(conf.get_redundancy_forecast_end()), item_store_old, df_redundancy_naive)

   status = " shorter, longer and redundancy forecast uploaded to s3 "
   local_dir = '/tmp/'
   upload_dataframe(df_ten, local_dir, 'tendayAhead_Naive.csv', conf.get_prod_bucket(), \
                    conf.get_shorter_forecast_upload_key())
   upload_dataframe(df_thirty, local_dir, 'thirtydayAhead_Naive.csv', conf.get_prod_bucket(), \
                    conf.get_longer_forecast_upload_key())
   upload_dataframe(df_redundancy, local_dir, 'thirtyfourAhead_Naive.csv', conf.get_prod_bucket(),\
                    conf.get_redundancy_forecast_upload_key())

   add_log.info(status + " - success ")


def get_item_store_data(conf):

   '''In this function sql query file is fetched , dataframe is returned
   '''
   status = " get item store data from database"
   location_num = conf.get_location_num()
   query_file = open(conf.get_item_store_combinations_sql_file_path(), 'r')
   query = query_file.read()
   query_file.close()
   #queryfunction = query % (location_num, location_num)
   query = query.replace("__database__", conf.get_database())
   query = query.replace("__initial_itemlevelcount_daily__", \
           conf.get_initial_itemlevelcount_table_name())
   query = query.replace("__lstm_combinations__", conf.get_lstm_combinations_table_name())
   query = query.replace("__location_num__", conf.get_location_num())
   print(query)
   item_store = execute_query(query, conf)
   item_store_df = pd.DataFrame( \
        item_store, columns=['location_num', 'product', 'max_business_date'])
   return item_store_df

def get_holidays_list(conf):

   status = " get holidays list from database"

   '''In this function sql query file is fetched , holidays business dates are returned in
   list'''

   query_file = open('core_forecast_inference_naive_daily_itemcount/holidays_list.sql', 'r')
   print ("reading done")
   query = query_file.read()
   print ("put down the query file to query done")
   query_file.close()

   query = query.replace("__database__", conf.get_database())
   query = query.replace("__inferencedates_daily__", conf.get_inferencedates_daily_table_name())
   list_holidays = execute_query(query, conf)
   list_holidays = pd.DataFrame(list_holidays, columns=['business_date']).astype( \
                                                                       str).values.T.tolist()[0]

   return list_holidays


def get_sales_data(conf, location_nums, product_picked):

   '''In this function sql query file is fetched , sales data for products picked is returned as
   pandas dataframe'''
   status = " get sales data from database "

   query_file = open('core_forecast_inference_naive_daily_itemcount/sales_data.sql', 'r')
   query = query_file.read()
   query_file.close()

   query = query.replace("__database__", conf.get_database())
   query = query.replace("__final_itemlevelcount_daily__", \
                         conf.get_final_itemlevelcount_daily_table_name())
   query = query.replace("__initial_itemlevelcount_daily__", \
                           conf.get_initial_itemlevelcount_table_name())
   query = query.replace("__location_num__", conf.get_location_num())
   query = query.replace("__product__", product_picked)

   sales_data = execute_query(query, conf)
   sales_data_df = pd.DataFrame(sales_data, columns=['aroundthanksgiving', 'aroundchristmas',\
                                           'onedaypriorchristmas_and_new_year', 'federalholiday',\
                                                  'holiday', 'blackfridaycheck', 'business_date',\
                                                  'dayofweek', 'sunday', 'location_num', \
                                                   'product', 'sum_daily_quantity', \
                                                   'federalholiday_name', 'generation_date'])
   return sales_data_df


def lambda_handler(event, context):


   '''This is main lambda function , naive logic is used based on products with sales,
   without sales -zero value is forecasted
   forecast datframes are uploaded to s3 bucket as CSV files
   Parameters
   ----------
   - event: dictionary
   - context : null parameter
   '''

    #try:
   status = " lambda handler get config object "

   ENV = os.getenv('ENV')
    
   with open('core_forecast_dataprep_incremental_itemcount/config.json') as config_params:
       config_dict = json.load(config_params)[ENV]
       config_dict['store_num'] = event['location_num']
       conf = Config.from_event(config_dict)

   add_log.info(status + " - success")
   print("success")

   today_date = datetime.datetime.now(tz=timezone('US/Pacific'))
   location_num = conf.get_location_num()

   status = " naive forecast for store - "
   add_log.info(status + str(location_num))

   item_store = get_item_store_data(conf)
   item_store_naive = item_store[pd.to_datetime(item_store['max_business_date']) >= (
        today_date - datetime.timedelta(days=30)).strftime("%Y-%m-%d")]
   print(item_store_naive)
   print(item_store['max_business_date'])

   list_holidays = get_holidays_list(conf)
   if len(item_store_naive) > 0:
       status = " forecast naive - with data in last 30 days"
       naive_len_grt_zero(item_store, item_store_naive, today_date, location_num, list_holidays,
                              conf)

       add_log.info(status + " - success")

   else:
       status = " forecast naive - with no data in last 30 days"
       naive_no_data_in_last_thirty_days(
               item_store, today_date, conf)
       add_log.info(status + " - success")
   print ("Done")

   #except:
      #add_log.error(status + " - failed")
       #add_log.error("naive forecast failed for store - " + str(location_num))
