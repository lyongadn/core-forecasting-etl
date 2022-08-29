"""
This code is generating forecast and summary jsons for each location and business_date
"""
import os
from base64 import b64decode
# from urllib.request import Request, urlopen
# from urllib.error import URLError, HTTPError
from collections import OrderedDict
import json
import datetime
import boto3
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import logging
from config import Config
from datetime import timedelta
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def upload_to_s3(prod_bucket, upload_path, local_path, gen_date, conf):
    """
    Input - local_path:str, prod_bucket:str, upload_path:str
    This method upload files from tmp folder to s3 at specified prod bucket and path
    """
    forecast = 'LSTM-14DaysAhead'
    if (conf.get_forecast_type() == 'baseline'):
        forecast = 'Baseline-14DaysAhead'
    s_3 = boto3.resource('s3')
    s_3.Object(prod_bucket, upload_path).put(Body=open(local_path, 'rb'), \
        Metadata={'Generation_Date':gen_date, 'Forecast_type': forecast})

# def send_slack_email(conf, message):
#     """
#     Input - conf:Config class object, message:str
#     This method sends the message to slack
#     """
#     # The Slack channel to send a message to stored in the slackChannel environment variable
#     kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
#     keys = kms_manager.get_secret_value(SecretId=conf.get_secret_key_slack())
#     credentials = json.loads(keys['SecretString'])
#     slack_channel = conf.get_slack_channel_QC()
#     hook_url = credentials['hook_url']

#     slack_message = {
#         'channel': slack_channel,
#         'text': message,
#         "username": conf.get_slack_username(),
#         "icon_emoji": conf.get_slack_emoji()
#     }
#     # print(slack_message)
#     req = Request(hook_url, json.dumps(slack_message).encode('utf-8'))
#     try:
#         response = urlopen(req)
#         response.read()
#         LOGGER.info("Message posted to %s", slack_message['channel'])
#     except HTTPError as error:
#         LOGGER.error("Request failed: %d %s", error.code, error.reason)
#     except URLError as error:
#         LOGGER.error("Server connection failed: %s", error.reason)

def get_business_date(query, connection, database, table, store_number, motive):
    """
    This method returns list of business_dates being forecasted for each store
    """
    with connection.cursor() as cur:
        query = query.replace('table', table).replace('database', database)
        query = query.replace('store_number', store_number)
        LOGGER.info(query)
        cur.execute(query)
        business_dates = cur.fetchall()
        dates = pd.DataFrame(list(business_dates), columns=['business_date'])
        date = pd.to_datetime(dates['business_date']).apply(lambda x: x.date())
        connection.commit()
        LOGGER.info('business_date has been fetched')
    if motive == 'fetchdates':
        return date
    if motive == 'last_year_date':
        return dates
        
        

def get_location_data_limits(connection, conf, weekday):
    '''Compute the Forecast limits based the data available in past 20 days'''
    query_dollarsales = open('core_forecast_csvtojson_2weekout/data_limits.sql', 'r').read()
    query_dollarsales = query_dollarsales%(conf.get_store_num(), weekday)
    query_dollarsales = query_dollarsales.replace('weekday_stats', \
        conf.get_dollarsales_weekday_table())
    dollarsales_data = execute_query(query_dollarsales, connection, \
        conf.get_database(), conf.get_store_num(), conf.get_weekly_stats_column())
    

    query_transcount = open('core_forecast_csvtojson_2weekout/data_limits.sql', 'r').read()
    query_transcount = query_transcount.replace('weekday_stats', \
        conf.get_transcount_weekday_table())

    transcount_data = execute_query(query_transcount%(conf.get_store_num(), weekday), \
     connection, conf.get_database(), conf.get_store_num(), conf.get_weekly_stats_column())

    query_itemcount = open('core_forecast_csvtojson_2weekout/data_limits.sql', 'r').read()
    query_itemcount = query_itemcount.replace('weekday_stats', \
        conf.get_itemcount_weekday_table())

    itemcount_data = execute_query(query_itemcount%(conf.get_store_num(), weekday), \
     connection, conf.get_database(), conf.get_store_num(), conf.get_weekly_stats_column())

    query_ingredient = open('core_forecast_csvtojson_2weekout/data_limits.sql', 'r').read()
    query_ingredient = query_ingredient.replace('weekday_stats', \
        conf.get_ingredient_weekday_table())

    ingredient_data = execute_query(query_ingredient%(conf.get_store_num(), weekday), \
     connection, conf.get_database(), conf.get_store_num(), conf.get_weekly_stats_column())
    print((transcount_data.head(),'transcount'))
    print((dollarsales_data.head(),'dollars'))
    print((itemcount_data.head(),'item'))
    print((ingredient_data.head(),'ingredient'))
    LOGGER.info('Got the forecast limits based on the past 20 days data after quering Aurora')
    return(compute_limits(transcount_data), compute_limits(dollarsales_data), \
            compute_limits(itemcount_data), compute_limits(ingredient_data))

def compute_limits(demand):
    '''
    The below function computes limits.
    Limits are based on median +/- 0.35*median of last 20 days data
    '''
    median = pd.to_numeric(demand['median'])
    iqr = pd.to_numeric(demand['iqr'])
    min_max_limit = pd.DataFrame(columns=['min_limit', 'max_limit'], \
        data={'min_limit':median-0.35*median, 'max_limit':median+0.35*median}, index=[0])
    print((min_max_limit.head()))

    LOGGER.info('Calculated the min and max forecast limit from the last 20 days data for QC')
    return min_max_limit

def execute_query(query, connection, database, store_number, columns, to_str_columns=None):
    """
    This method loads data from s3 at specified path to aurora
    in specified database and table
    """
    with connection.cursor() as cur:
        query = query.replace('store_number', str(store_number)).replace('database', database)
        #print (query)
        cur.execute(query)
        data = cur.fetchall()
        final_data = pd.DataFrame(list(data), columns=columns)
        if to_str_columns is not None:
            final_data[to_str_columns] = final_data[to_str_columns].astype('str')
        connection.commit()
    return final_data


def reading_json(local_path, business_date, location_num):
    """
    funcation is reading the jsons based on date and location_Num
    """
    StoreNumber = location_num
    with open(local_path+business_date+'.json') as json_file:
        json_10days_ahead = pd.DataFrame(json.load(json_file))
        print ("Json data print will start") ##Comments
        print((json_10days_ahead.head()))    ##Comments
        print ("Json data printed") ##Comments
    ################### Item Json Data ########################
    menuitem_10daysahead = pd.DataFrame(json_10days_ahead.loc['SalesItemDetail', 'Forecast'])
    print((menuitem_10daysahead.head())) ##Comments
    print ("Reading json menu item printed") ##Comments
    jsonitem_data = pd.DataFrame(columns=['location_num', 'productcount', 'Rowscount', \
        'total_forecast'], data={'location_num':StoreNumber, \
        'productcount':len(menuitem_10daysahead['SalesItemCode'].unique()), \
        'Rowscount':len(menuitem_10daysahead), \
    'total_forecast':pd.to_numeric(menuitem_10daysahead['TransactionCount']).sum()}, index=[0])

    jsonitem_data['RowsPerProduct'] = jsonitem_data['Rowscount']/jsonitem_data['productcount']
    jsonitem_data['generation_date'] = \
    json_10days_ahead.loc['SalesItemDetail']['GenerationDate'].split('T')[0]

    ################### Item Json Data - 160001 ########################
    menuitem_10daysahead_160001 = \
    menuitem_10daysahead[menuitem_10daysahead['SalesItemCode'] == '160001']

    jsonitem_data_160001 = pd.DataFrame(columns=['location_num', 'product', 'Rowscount', \
     'total_forecast'], data={'location_num':StoreNumber, \
     'product':menuitem_10daysahead_160001['SalesItemCode'].unique(), \
     'Rowscount':len(menuitem_10daysahead_160001), \
     'total_forecast':pd.to_numeric(menuitem_10daysahead_160001['TransactionCount']).sum()}, \
     index=[0])
    print((jsonitem_data_160001.head()))  ##Comments

     ################### Ingredient Json Data ########################
    ingredient_10daysahead = pd.DataFrame(json_10days_ahead.loc['InventoryItemDetail', 'Forecast'])

    jsoningredient_data = pd.DataFrame(columns=['location_num', 'ingredientcount', 'Rowscount', \
     'total_forecast'], data={'location_num':StoreNumber, \
     'ingredientcount':len(ingredient_10daysahead['ItemCode'].unique()), \
     'Rowscount':len(ingredient_10daysahead), \
     'total_forecast':pd.to_numeric(ingredient_10daysahead['TransactionCount']).sum()}, index=[0])

    jsoningredient_data['RowsPerIngredient'] = \
    jsoningredient_data['Rowscount']/ jsoningredient_data['ingredientcount']
    jsoningredient_data['generation_date'] = \
    json_10days_ahead.loc['InventoryItemDetail']['GenerationDate'].split('T')[0]

    ################### Ingredient Json Data ########################
    ingredient_10daysahead_1 = ingredient_10daysahead[ingredient_10daysahead['ItemCode'] == '1']

    jsoningredient_data_1 = pd.DataFrame(columns=['location_num', 'ingredient_id', 'Rowscount', \
        'total_forecast'], data={'location_num':StoreNumber, \
        'ingredient_id':ingredient_10daysahead_1['ItemCode'].unique(), \
        'Rowscount':len(ingredient_10daysahead_1),\
        'total_forecast':pd.to_numeric(ingredient_10daysahead_1['TransactionCount']).sum()}, \
        index=[0])

    ################### Dollarsales and transcount Json Data ########################
    metric_10daysahead = pd.DataFrame(json_10days_ahead.loc['MetricDetail', 'Forecast'])

    jsonmetric_data = pd.DataFrame(columns=['location_num', 'Rowscount', 'Sales_forecast', \
        'Transcount_forecast'], data={'location_num':StoreNumber, \
        'Rowscount':len(metric_10daysahead), \
        'Sales_forecast':pd.to_numeric(metric_10daysahead['Sales']).sum(), \
        'Transcount_forecast':pd.to_numeric(metric_10daysahead['TransactionCount']).sum()}, \
         index=[0])

    jsonmetric_data['generation_date'] = \
    json_10days_ahead.loc['MetricDetail']['GenerationDate'].split('T')[0]
    generation_date_metadata = json_10days_ahead.loc['MetricDetail']['GenerationDate']

    return(jsonitem_data, jsoningredient_data, jsonmetric_data, menuitem_10daysahead,\
    ingredient_10daysahead, metric_10daysahead, jsonitem_data_160001, jsoningredient_data_1, \
           generation_date_metadata)


def readin_Jsonsummary(summaryjson_local_path, business_date, location_num):
    StoreNumber = location_num
    with open(summaryjson_local_path+business_date+'.json') as json_file:
        json_14days_ahead = pd.DataFrame(json.load(json_file))
        generation_date_metadata = json_14days_ahead.loc['MetricDetail']['GenerationDate']
    return(generation_date_metadata)

def checkflatforecast_andtimeinterval(menuitem_10daysahead, \
ingredient_10daysahead, metric_10daysahead, alerts_csv, flag):
    """
    checks flat forecast i.e., if the forecast is same for all 96 time intervals
    """
    alert_types = ['ItemCount_flat', 'Ingredient_flat', 'TransCount_flat', 'Dollarsales_flat']
    for alert_type in alert_types:
        alerts_csv[alert_type] = 0

    data = menuitem_10daysahead[menuitem_10daysahead['SalesItemCode'] == '160001']
    print((data.head())) ##Comment
    print("dataprinted") ##Comment
    print((menuitem_10daysahead.head())) ##Comment
    print('menuItemprinted') ##Comment
    try:
        print((data['IntervalStart'])) ##Comment
    except:
        print("no data there")
    if data['TransactionCount'].unique().shape[0] == 2:
        if len(data[data['TransactionCount'] == data['TransactionCount'].unique()[1]]) == 65:
            flag = 1
            alerts_csv['ItemCount_flat'] = 1
    print((flag,'160001 check flat forecast'))
    #if data['IntervalStart'].shape[0] != 96:
        #temp_time.append(prod)
        #temp_var += alert_slack
        #temp_var += '\nItemCount does not have forecast for 96 time intervals for product : 160001\n'
        #flag = 1
    print((flag,'160001 check rows'))

    data = ingredient_10daysahead[ingredient_10daysahead['ItemCode'] == '1']
    if data['TransactionCount'].unique().shape[0] == 2:
        if len(data[data['TransactionCount'] == data['TransactionCount'].unique()[1]]) == 65:
            flag = 1
            alerts_csv['Ingredient_flat'] = 1

    #if data['IntervalStart'].unique().shape[0] != 96:
        #temp_var += alert_slack
        #temp_var += '\nIngredientCount does not have forecast for 96 time intervals for ingredinet : 1\n'
        #flag = 1

    if metric_10daysahead['TransactionCount'].unique().shape[0] == 2:
        if len(metric_10daysahead[metric_10daysahead['TransactionCount'] == \
                            metric_10daysahead['TransactionCount'].unique()[1]]) == 65:
            flag = 1
            alerts_csv['TransCount_flat'] = 1


    if metric_10daysahead['Sales'].unique().shape[0] == 2:
        if len(metric_10daysahead[metric_10daysahead['Sales'] == \
            metric_10daysahead['Sales'].unique()[1]]) == 65:
            flag = 1
            alerts_csv['Dollarsales_flat'] = 1


    #if metric_10daysahead['IntervalStart'].unique().shape[0] != 96:
        #temp_var += alert_slack
        #temp_var += '\nDollarSales & TransactionCount does not have forecast for 96 time intervals\n'
        #flag = 1
    return flag

def quality_check(data, filtered_data, forecast_type, metric, location_num, business_date, \
    limits_data, alerts_csv):
    """
    this function checks the forecast limit based on location and business_date for a json
    """
    flag = 0
    flag_greater_limit = 0 
    # temp_var += "\n"
    # temp_var += forecast_type+" Forecast :" + str(pd.to_numeric(filtered_data[metric]).sum())
    # temp_var += "\n"+ forecast_type+" forecast limits :"
    # temp_var += " Min_limit - "+str(limits_data['min_limit'][0])
    # temp_var += " Max_limit - "+str(limits_data['max_limit'][0])
    # temp_var += "\n"
    print((len(data), forecast_type))  ##Comments
    print((flag, "flag1",str(limits_data['min_limit'][0]),str(limits_data['max_limit'][0])))  ##Comments
    print((data.head()))         ##Comments
    
    alerts_csv[forecast_type+"_VAL"] = pd.to_numeric(filtered_data[metric]).sum()
    alerts_csv[forecast_type+"_MIN_LIMIT"] = limits_data['min_limit'][0]
    alerts_csv[forecast_type+"_MAX_LIMIT"] = limits_data['max_limit'][0]
    alerts_csv[forecast_type+"_forecast_generated"] = 1
    alerts_csv[forecast_type+"_sunday_zeros"] = 1

    alert_types = ["_forecast_generated_successfully", "_less_than_min", "_less_than_90%_min", "_greater_than_max", "_greater_than_1.5_max"]
    for alert_type in alert_types:
        alerts_csv[forecast_type+alert_type] = 0
    
    if len(data) != 1:
    #     temp_var += alert_slack
    #     temp_var += "\n"
    #     temp_var += forecast_type+" forecast is not generated for the store :"+ location_num +"\n"
    #    #temp_var +="No of locations with missing forecast are " + str(len(data) - len(data))
        flag = 1
        alerts_csv[forecast_type+"_forecast_generated"] = 0
        print((flag, "flag1"))
    #else:
        #if pd.to_numeric(data['flag']).sum() != 0:
            #print("wentinside1")
            #temp_var += alert_slack
            #temp_var += "\n"
            #temp_var += forecast_type+"forecast is not available for 96 intervals at 15min level for location_num:"
            #location_num_badforecast = data[data['flag'] == '1']['location_num']
            #location_num_badforecast = [str(x) for x in location_num_badforecast]
            #location_num_badforecast = ",".join(location_num_badforecast)
            #temp_var += location_num_badforecast
            #temp_var += "\n"
            #flag = 1 
            #print(flag, "flag2")
    else:
        print("wentinside2")
        if pd.to_datetime(business_date).weekday() == 6:
            print("wentinside3")
            if pd.to_numeric(data[metric]).sum() == 0:
                print("wentinside4")
                alerts_csv[forecast_type+"_forecast_generated_successfully"] = 0
            else:
                print("wentinside5")
                alerts_csv[forecast_type+"_sunday_zeros"] = 0
                flag = 1
        else:
            print("wentinside6")
            if max(0, limits_data['min_limit'][0]) < \
                pd.to_numeric(filtered_data[metric]).sum() < limits_data['max_limit'][0]:
                print("wentinside7")
                alerts_csv[forecast_type+"_forecast_generated_successfully"] = 1
                print((limits_data['max_limit'], "343"))
            else:
                alerts_csv[forecast_type+"_forecast_generated_successfully"] = 1                
                print("wentinside8")
                if (pd.to_numeric(filtered_data[metric]).sum() <= max(0, \
                    limits_data['min_limit'][0])):
                    print("wentinside9")
                    lowerlimit = (limits_data['min_limit'][0] - (0.1*limits_data['min_limit'][0]))
                    if (pd.to_numeric(filtered_data[metric]).sum() <= max(0,lowerlimit)):
                        print("wentinside10")
                        alerts_csv[forecast_type+"_less_than_90%_min"] = 1
                        alerts_csv[forecast_type+"_less_than_min"] = 1
                        flag = 1
                    else:
                        print("wentinside11")
                        alerts_csv[forecast_type+"_less_than_min"] = 1
                        flag_greater_limit = 1
                else:
                    print("wentinside12")
                    if (pd.to_numeric(filtered_data[metric]).sum() >= \
                        1.5*limits_data['max_limit'][0]):
                        if (pd.to_numeric(filtered_data[metric]).sum() >= \
                        1.5*limits_data['max_limit'][0]):
                            alerts_csv[forecast_type+"_greater_than_1.5_max"] = 1
                            alerts_csv[forecast_type+"_greater_than_max"] = 1
                            flag = 1
                        else:
                            print("wentinside13")
                            alerts_csv[forecast_type+"_greater_than_max"] = 1
                            flag_greater_limit = 1
    print((flag, flag_greater_limit, 'quality check flag')) ## Comments
    print("Alerts for "+forecast_type+" are :- ",alerts_csv)
    return (flag, flag_greater_limit)

def json_qc(connection, conf, business_date, location_num, weekday):
    """ Function is responsible for checking the forecast limits and doing the quality check of the data"""
    # temp_var_metrics = " "
    # temp_var_dollars = " "
    # temp_var_trans = " "
    # temp_var_item = " "
    # temp_var_ingre = " "
    # temp_var =" "
    # temp_var += "store number : " + location_num + "\n"
    # temp_var += "json_qc for business_date : " + business_date + "\n"
    # alert_slack = "\n *alert_slack* \n"
    flag = 0
    dfitem_json = pd.DataFrame()
    dfingredient_json = pd.DataFrame()
    dfmetric_json = pd.DataFrame()
    dfitem_json160001 = pd.DataFrame()
    dfingredient_json1 = pd.DataFrame()

    dfitem_json, dfingredient_json, dfmetric_json, menuitem_10daysahead, ingredient_10daysahead, \
    metric_10daysahead, dfitem_json160001, dfingredient_json1, gen_date = reading_json( \
                                            conf.get_json_local_path(), business_date, location_num)
    gen_date_forecast= gen_date
    gensummarydate = readin_Jsonsummary(conf.get_summaryjson_local_path(), business_date,location_num)
    #gen_date_summary_forecast = gensummarydate

    alert = {
        "location_num" : location_num,
        "business_date" : business_date,
        "generation_date" : gen_date.split('T')[0]
    }

    flag = checkflatforecast_andtimeinterval(menuitem_10daysahead, \
                                ingredient_10daysahead, metric_10daysahead, \
                                alert, flag)
    month = business_date.split("-")[0]
    print (flag, "Direct check") ##Comments

    transcountlimits_data, dollarsaleslimits_data, itemquantity_limits_data, ingredientlimits_data\
                            = get_location_data_limits(connection, conf, weekday)

    # Checking the expected number of rows (=96) at 15min level - Itemcount 
    #dfitem_json.loc[dfitem_json['RowsPerProduct'] == 96, 'flag'] = '0'
    #dfitem_json.loc[dfitem_json['RowsPerProduct'] != 96, 'flag'] = '1'

    # Checking the expected number of rows (=96) at 15min level - Ingredient
    #dfingredient_json.loc[dfingredient_json['RowsPerIngredient'] == 96, 'flag'] = '0'
    #dfingredient_json.loc[dfingredient_json['RowsPerIngredient'] != 96, 'flag'] = '1'

    # Checking the expected number of rows (=96) at 15min level - Dollars and Transcount 
    #dfmetric_json.loc[dfmetric_json['Rowscount'] == 96, 'flag'] = '0'
    #dfmetric_json.loc[dfmetric_json['Rowscount'] != 96, 'flag'] = '1'

    #temp_var += "\n Transcount Forecast :" + \
    #str(pd.to_numeric(dfmetric_json['Transcount_forecast']).sum())
    #temp_var += " \n Transcount forecast limits :"
    #temp_var += " Min_limit - "+str(transcountlimits_data['min_limit'][0])
    #temp_var += " Max_limit - "+str(transcountlimits_data['max_limit'][0])
    #temp_var += "\n \n Dollarsales Forecast :" + \
    #str(pd.to_numeric(dfmetric_json['Sales_forecast']).sum())
    #temp_var += "\n Dollarsales forecast limits :"
    #temp_var += " Min_limit - "+str(dollarsaleslimits_data['min_limit'][0])
    #temp_var += " Max_limit - "+str(dollarsaleslimits_data['max_limit'][0])
    #temp_var += "\n \n ItemCount Forecast :" + \
    #str(pd.to_numeric(dfitem_json160001['total_forecast']).sum())
    #temp_var += "\n ItemCount forecast limits :"
    #temp_var += " Min_limit - "+str(itemquantity_limits_data['min_limit'][0])
    #temp_var += " Max_limit - "+str(itemquantity_limits_data['max_limit'][0])
    #temp_var += "\n \n Ingredient Forecast :" + \
    #str(pd.to_numeric(dfingredient_json1['total_forecast']).sum())
    #temp_var += "\n Ingredient forecast limits :"
    #temp_var += " Min_limit - "+str(ingredientlimits_data['min_limit'][0])
    #temp_var += " Max_limit - "+str(ingredientlimits_data['max_limit'][0])
    
    flag_final_dollars= flag_final_trans= flag_final_item= flag_final_ingre=0
    flag_greater_limit_dollars= flag_greater_limit_trans= flag_greater_limit_item= flag_greater_limit_ingre=0
    flag_final_dollars, flag_greater_limit_dollars = quality_check(dfmetric_json, dfmetric_json, \
        'Dollarsales', 'Sales_forecast', \
        conf.get_store_num(), business_date, dollarsaleslimits_data, alert)
    print(flag_final_dollars, flag_greater_limit_dollars,'flag and flag greater limit dollars') ##Comments
    LOGGER.info("DollarSales forecast quality_check has been completed")
    flag_final_trans, flag_greater_limit_trans = quality_check(dfmetric_json, dfmetric_json, \
        'TransCount', 'Transcount_forecast', \
        conf.get_store_num(), business_date, transcountlimits_data, alert)
    LOGGER.info("Transcount forecast quality_check has been completed")
    print(flag_final_trans, flag_greater_limit_trans,'flag and flag greater limit trans') ##Comments
    flag_final_item, flag_greater_limit_item = quality_check(dfitem_json, dfitem_json160001, \
        'ItemCount', 'total_forecast', \
     conf.get_store_num(), business_date, itemquantity_limits_data, alert)
    LOGGER.info("ItemCount forecast quality_check has been completed")
    print(flag_final_item, flag_greater_limit_item,'flag and flag greater limit item') ##Comments
    flag_final_ingre, flag_greater_limit_ingre = quality_check(dfingredient_json, dfingredient_json1, \
        'Ingredient', 'total_forecast', \
     conf.get_store_num(), business_date, ingredientlimits_data, alert)
    LOGGER.info("Ingredient forecast quality_check has been completed")
    print(flag_final_ingre, flag_greater_limit_ingre,'flag and flag greater limit ingre') ##Comments
    print(flag_final_dollars, flag_final_trans, flag_final_item, flag_final_ingre,\
          flag_greater_limit_dollars, flag_greater_limit_trans, flag_greater_limit_item, flag_greater_limit_ingre,'flag and flag final') ##comments
    flag = flag+flag_final_dollars+flag_final_trans+flag_final_item+flag_final_ingre
    flag_greater_limit = flag_greater_limit_dollars+ flag_greater_limit_trans+ flag_greater_limit_item+ flag_greater_limit_ingre
    print (flag)
    if flag_greater_limit> 0 :
        flag_greater_limit= 1
    else:
        flag_greater_limit= 0
    if flag > 0:
        flag = 0
        return ("Failed", business_date, flag_greater_limit, gen_date_forecast, gensummarydate, alert)
    else:
        flag = 0
        return ("Success", business_date, flag_greater_limit, gen_date_forecast, gensummarydate, alert)


def lambda_handler(event, context):
    """Lambda function"""
    ENV = os.getenv('ENV')
    
    with open('core_forecast_csvtojson_2weekout/config.json') as config_params:
        config_dict = json.load(config_params)[ENV]
        config_dict['store_num'] = event['store_num']
        conf = Config.from_event(config_dict)

    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=conf.get_secret_key())
    credentials = json.loads(keys['SecretString'])

    database = conf.get_database()
    if (conf.get_forecast_type() == 'baseline'):
        database = conf.get_baseline_database()

    print("Store is", conf.get_store_num())
    print("Forecast Type is", conf.get_forecast_type())

    connection = pymysql.connect(host=conf.get_custom_host_link(),
                                 user=credentials['username'],
                                 password=credentials['password'],
                                 db=database)
    LOGGER.info("Successfully connected to Aurora")

    query_dates = open('core_forecast_csvtojson_2weekout/get_dates.sql', 'r')
    dates = query_dates.read()
    business_dates = get_business_date(dates, connection, database, \
        conf.get_15min_dollar_table(), conf.get_store_num(), 'fetchdates')
    print("Dates are",business_dates)

    for dates in business_dates:
        
        lastyear_date = open('core_forecast_csvtojson_2weekout/get_last_year_date.sql', 'r')
        lastyear_dates = lastyear_date.read()
        lastyear_dates = lastyear_dates.replace('%s', str(dates))
        lastyear_date_df = get_business_date(lastyear_dates, connection, conf.get_database(), \
        conf.get_15min_dollar_table(), conf.get_store_num(), 'last_year_date')
        print((lastyear_date_df['business_date'][0]))
        lastyear_date = (lastyear_date_df['business_date'][0])

        daily_dollartranscount = open('core_forecast_csvtojson_2weekout/daily_dollartranscount.sql', 'r').read()
        daily_dollartranscount = daily_dollartranscount.replace('15min_dollar_table',\
            conf.get_15min_dollar_table()).replace('final_table',\
            conf.get_daily_final_table()).replace('%s', str(dates)).replace('date_last_year',\
             str(lastyear_date))
        dollartranscount_daily = execute_query(daily_dollartranscount, connection, \
            database, conf.get_store_num(), conf.get_daily_dollartranscount_columns(), \
            conf.get_to_str_daily_dollartranscount_columns())

        daily_dollartranscount = \
        dollartranscount_daily['StoreNumber'].apply(lambda x: '0'*(5-len(str(x)))+str(x))
        store_list = dollartranscount_daily['StoreNumber']

        if len(store_list) < 1:
            LOGGER.info("No data available for selected business_dates")
        else:
            date = dollartranscount_daily['BusinessDay'][0][:10]
            date = datetime.date.strftime(pd.to_datetime(date, format='%Y-%m-%d'), "%m-%d-%Y")
            
            LOGGER.info('Daily forecast for dollarsales and transcount is done for '+conf.get_store_num())
            
            qry_15min_dollartranscount = open('core_forecast_csvtojson_2weekout/15min_dollartranscount.sql', 'r').read()
            qry_15min_dollartranscount = qry_15min_dollartranscount.replace('15min_dollar_table', \
                conf.get_15min_dollar_table()).replace('final_table_15min', \
                conf.get_15min_final_table()).replace('%s', str(dates)).replace('date_last_year',\
             str(lastyear_date))
            dollartranscount_15min = execute_query(qry_15min_dollartranscount, connection, \
                database, conf.get_store_num(), \
                conf.get_15min_dollartranscount_columns(), \
                conf.get_to_str_15min_dollartranscount_columns())
            LOGGER.info('15min forecast for dollarsales and transcount is done for '+conf.get_store_num())

            qry_15min_itemcount = open('core_forecast_csvtojson_2weekout/15min_itemcount.sql', 'r').read()
            qry_15min_itemcount_final = qry_15min_itemcount
            qry_15min_itemcount_final = qry_15min_itemcount_final.replace('15min_itemcount_table', \
                conf.get_15min_itemcount_table())
            qry_15min_itemcount_final = qry_15min_itemcount_final.replace('%s', str(dates)).replace('date_last_year',\
             str(lastyear_date))
            itemcount_15min = execute_query(qry_15min_itemcount_final, connection, \
                database, conf.get_store_num(), conf.get_15min_itemcount_columns(), \
                conf.get_to_str_15min_itemcount_columns())
            LOGGER.info('15min forecast for Itemcount is done for'+conf.get_store_num())

            qry_15min_ingredient = open('core_forecast_csvtojson_2weekout/15min_ingredient.sql', 'r').read()
            qry_15min_ingredient = qry_15min_ingredient.replace('15min_ingredient_table', \
                conf.get_15min_ingredient_table())
            qry_15min_ingredient = qry_15min_ingredient.replace('%s', str(dates)).replace('date_last_year',\
             str(lastyear_date))
            ingredient_15min = execute_query(qry_15min_ingredient, connection, \
                database, conf.get_store_num(), conf.get_15min_ingredient_columns(), \
                conf.get_to_str_15min_ingredient_columns())
            LOGGER.info('15min forecast for Ingredient is done for'+conf.get_store_num())
            
            forecast_summary = [OrderedDict(row) for i, row in dollartranscount_daily.iterrows()]
            summary = forecast_summary[0]
            MetricDetail = [OrderedDict(row) for i, row in dollartranscount_15min.iterrows()]
            summary.update(OrderedDict([("SalesItemDetail", [])]))
            summary.update(OrderedDict([("InventoryItemDetail", [])]))
            summary.update(OrderedDict([("MetricDetail", MetricDetail)]))
            forecast_main_summary = {}
            forecast_main_summary['Forecast'] = summary
            forecast_main_summary['GenerationDate'] = \
            (datetime.datetime.now()-timedelta(seconds=10)).strftime("%Y-%m-%dT%H:%M:%S.%m0Z")
            #gen_date_summary=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%m0Z")
            with open(conf.get_summaryjson_local_path()+date+'.json', 'w') as outfile:
                json.dump(forecast_main_summary, outfile)
                
            LOGGER.info('Summary JSON is generated and uploaded in temp folder for'+conf.get_store_num())
            
            forecast = [OrderedDict(row) for i, row in dollartranscount_daily.iterrows()]
            all_forecast = forecast[0]
            MetricDetail = [OrderedDict(row) for i, row in dollartranscount_15min.iterrows()]
            all_forecast.update(OrderedDict([("MetricDetail", MetricDetail)]))
    
            SalesItemDetail = [OrderedDict(row) for i, row in itemcount_15min.iterrows()]
            all_forecast.update(OrderedDict([("SalesItemDetail", SalesItemDetail)]))
    
            InventoryItemDetail = [OrderedDict(row) for i, row in ingredient_15min.iterrows()]
            all_forecast.update(OrderedDict([("InventoryItemDetail", InventoryItemDetail)]))
            forecast_main = {}

            forecast_main['Forecast'] = all_forecast
            forecast_main['GenerationDate'] = \
            datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%m0Z")
            #gen_date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%m0Z")
                        #store_number = int(event['store_number'])
            with open(conf.get_json_local_path()+date+'.json', 'w') as outfile:
                print((conf.get_json_local_path()+date+'.json'))
                json.dump(forecast_main, outfile)
            LOGGER.info('Forecast JSON is generated for '+conf.get_store_num())
                
    LOGGER.info("JSON quality_check will start now")
    
    alerts = []

    for dates in range(len(business_dates)):
        try :
            print(dates)
            print(business_dates)
            """
            Running JSON quality_check for all business_dates and stores
            """
            status, date, flag_greater_limit, gen_date_forecast, \
            gen_date_summary_forecast, alert= json_qc(connection, conf, \
                        business_dates[dates].strftime("%m-%d-%Y"), \
                        str(conf.get_store_num()), business_dates[dates].weekday())
            alerts.append(alert)

            if status == 'Success':
                """
                Uploading Forecasting JSON
                """
                upload_to_s3(conf.get_json_bucket(), \
                conf.get_upload_path()+dollartranscount_daily['StoreNumber'][0]+'/'+'demand-'+\
                business_dates[dates].strftime("%m-%d-%Y")+'.json', \
                conf.get_json_local_path()+date+'.json', gen_date_forecast, conf)
                LOGGER.info("Forecast JSON uploaded")
                
                """
                Uploading summary JSON 
                """
                #upload_to_s3(conf.get_json_bucket(), \
                #conf.get_summary_upload_path()+dollartranscount_daily['StoreNumber'][0]+'/'+'demand-'+\
                #business_dates[dates].strftime("%m-%d-%Y")+'.json', \
                #conf.get_summaryjson_local_path()+date+'.json', gen_date_summary_forecast, conf)
                #LOGGER.info("Summary JSON uploaded")
                
            else:
                upload_to_s3(conf.get_test_bucket(), \
                conf.get_upload_path_test()+dollartranscount_daily['StoreNumber'][0]+'/'+'demand-'+\
                business_dates[dates].strftime("%m-%d-%Y")+'.json', \
                conf.get_json_local_path()+date+'.json', gen_date_forecast, conf)
                LOGGER.info("Forecast JSON is not in limits and uploaded to secondary(prod-q-forecasting) bucket")
                
                #upload_to_s3(conf.get_test_bucket(), \
                #conf.get_summary_upload_path_test()+dollartranscount_daily['StoreNumber'][0]+'/'+'demand-'+\
                #business_dates[dates].strftime("%m-%d-%Y")+'.json', \
                #conf.get_summaryjson_local_path()+date+'.json', gen_date_summary_forecast, conf)
                #LOGGER.info("JSON data is falling out of limits and uploaded to test bucket")
                
            LOGGER.info("JSON upload completed for"+str(conf.get_store_num()))

        except Exception as e: 
            print("Exception", e)
            import traceback
            traceback.print_exc()

    connection.close()
    
    alerts_df = pd.DataFrame(alerts)
    print(alerts_df)
    alerts_df.to_csv(f"s3://{conf.get_alert_bucket()}/{conf.get_alert_path()+str(conf.get_store_num())}_alerts.csv",index=False)
    LOGGER.info("Saved the alerts_dataframe in S3.")
