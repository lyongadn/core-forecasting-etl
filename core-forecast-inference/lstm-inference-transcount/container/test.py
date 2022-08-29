#n[20]:


# Packages Needed

# Machine Learning Packages
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler, RobustScaler
from sklearn.externals import joblib
from numpy import concatenate
from sklearn.preprocessing import LabelEncoder
from keras.models import load_model

# File Handling
import boto3
from boto3.s3.transfer import S3Transfer
from io import StringIO
import os
import json
#import pickle

# Calendar / Time computation packages
import time
from datetime import datetime, date

# Garbage Collector
import gc
secret = None
from keras import backend as K

# Making connection to Athena
xacct_client = boto3.client('athena', region_name='us-east-1')
# s3_output = 's3://cfa-forecasting/forecast/lstm/temp_files/'

#517,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path
# csv_bucket = 'cfa-forecasting'
athena_csv_filepath='forecast/lstm/temp_files/'



# #model properties
bucket_name = 'cfa-forecasting'
mp_path = 'forecast/lstm/model-object/'
# #h5 file
# bucket_m = 'cfa-forecasting'
h5_path = 'forecast/lstm/model-object/'

# #upload predictions
# upload_bucket='cfa-forecasting'
upload_path='forecast/lstm/dollarsales/daily/'
# In[27]:


def make_it_zero(DataFrame):
    '''
    In this UDF, we make transaction count for all sundays and holidays as 0 
    since all CFA stores are closed on these days.
    
    Parameters
    ----------
     - DataFrame: Master Dataframe
    
    Returns: 
    ----------
    - DataFrame: Transformed Dataframe where transaction count is 0 for sundays and holidays
    '''
    DataFrame.loc[DataFrame['sunday'] == 1,'trans_count'] = 0
    DataFrame.loc[DataFrame['holiday'] == 1,'trans_count'] = 0
    return DataFrame

def series_to_supervised(data, n_in=1, n_out=1, dropnan=True):
    '''
    In this function, we convert a time series dataframe into a supervised dataframe 
    where each column represent value of variables at a particular time period such as t, t-1... t-k, t+1... t+n

    Parameters
    ----------
     - Dataframe: Time Series dataframe
     - n_in: Lookback time periods
     - n_out: Horizon time periods
     - dropnan: Remove all nan from the dataframe
     
    Returns: 
    ----------
    - Supervised Dataframe containing value of variables at different time periods
    '''
    n_vars = 1 if type(data) is list else data.shape[1]
    df = pd.DataFrame(data)
    cols, names = list(), list()
    # input sequence (t-n, ... t-1)
    for i in range(n_in, 0, -1):
        cols.append(df.shift(i))
        #print ('n_in', len(cols))
        names += [('var%d(t-%d)' % (j+1, i)) for j in range(n_vars)]
    # forecast sequence (t, t+1, ... t+n)
    for i in range(0, n_out):
        cols.append(df.shift(-i))
        #print ('n_out', len(cols))
        if i == 0:
            names += [('var%d(t)' % (j+1)) for j in range(n_vars)]
        else:
            names += [('var%d(t+%d)' % (j+1, i)) for j in range(n_vars)]
    # put it all together
    agg = pd.concat(cols, axis=1)
    agg.columns = names
    # drop rows with NaN values
    if dropnan:
        agg.dropna(inplace=True)
    return agg

def robust_scaler_transform(unscaled_arr, med_list, iqr_list):
    '''
    In this function, an unscaled array is transformed 
    based on the values of median and IQR passed to it
    
    Parameters
    ----------
    - unscaled_arr: The multidimensional array of raw values
    - med_list: A list containing median for each feature used in the model
    - iqr_list: A list containing IQR for each feature used in the model
    
    Returns: 
    ----------
    - scaled_arr: Returns the scaled array
    '''
    # finding the nos of features
    nos_features = unscaled_arr.shape[1]
    scaled_arr = np.empty([unscaled_arr.shape[0], unscaled_arr.shape[1]])
    for i in range(nos_features):
        med = med_list[i]
        iqr = iqr_list[i]
        if iqr != 0:
            scaled_arr[:,i] = (unscaled_arr[:,i] - med)/ iqr
        else:
            scaled_arr[:,i] = unscaled_arr[:,i]  
    return(scaled_arr)

def robust_scaler_inverse_transform(yhat, med_list, iqr_list):
    '''
    In this function, a scaled array is transformed 
    based on the values of median and IQR passed to it
    
    Parameters
    ----------
    - yhat: The multidimensional array of scaled values
    - med_list: A list containing median for each feature used in the model
    - iqr_list: A list containing IQR for each feature used in the model
    
    Returns: 
    ----------
    - inv_yhat: Returns the raw data array
    '''
    # finding the nos of features
    nos_features = yhat.shape[1]
    inv_yhat = np.empty([yhat.shape[0], yhat.shape[1]])
    for i in range(nos_features):
        med = med_list[i]
        iqr = iqr_list[i]
        if iqr != 0:
            inv_yhat[:,i] = (yhat[:,i] * iqr) + med
        else:
            inv_yhat[:,i] = yhat[:,i]
    return(inv_yhat)


# In[41]:


def sales_inference(store_num,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path):
    '''
    In this function, a location_number is passed as paramter to return its sales prediction for n days ahead.
    The prediction for nth day is saved at the s3 path. The prediction output format is location_num, businessdate, forcast
    
    Parameters
    ----------
    - store_num: Location_num of the store
    '''

    csv_bucket=bucket_name
    bucket_mp=bucket_name
    bucket_m=bucket_name
    upload_bucket=bucket_name
    athena_csv_filepath=athena_csv_filepath
    mp_path=mp_path
    h5_path=h5_path
    upload_path=upload_path

    i = store_num
    start = time.time()
    
    

    # Fetch data based on the below query
    query = """
    select aroundthanksgiving,
    aroundchristmas ,
    onedaypriorchristmas_and_new_year   ,
    federalholiday,
    holiday ,
    blackfridaycheck,
    business_date,
    dayofweek   ,
    cast(sunday as integer)sunday,
    location_num,
    product,
    productname,    
    sales_sub_total,
    discounted_sales_sub_total,
    trans_count

    from ml_preprod.final_dollarsalesandtranscount_daily
    where cast(location_num as integer)=%s and cast(business_date  as date )>date_add('day',-15, (select max(cast(business_Date as date)) from ml_preprod.final_dollarsalesandtranscount_daily where 
                                        cast(location_num as integer)=%s)) and cast(business_date  as date )<=(select max(cast(business_Date as date)) from ml_preprod.final_dollarsalesandtranscount_daily where 
                                        cast(location_num as integer)=%s)

    union all
    select 
    aroundthanksgiving,aroundchristmas  ,onedaypriorchristmas_and_new_year, federalholiday, holiday,blackfridaycheck,
    business_date,dayofweek,CASE
       WHEN cast(location_num as integer) IN (652,375,517)
           AND cast(dayofweek as integer) IN (6,7) THEN
       1
       ELSE cast(sunday as integer)
       END AS sunday,location_num,product,productname,sales_sub_total,discounted_sales_sub_total,  trans_count

    from (select * from (select distinct location_num from ml_preprod.final_dollarsalesandtranscount_daily where cast(location_num as integer)= %s)a cross join ml_preprod.inferencedates_daily)A
    where cast(business_date as date) >(select max(cast(business_Date as date)) from ml_preprod.final_dollarsalesandtranscount_daily where 
                                        cast(location_num as integer)=%s) and cast(business_date as date) <=date_add('day',10,(select max(cast(business_Date as date)) from ml_preprod.final_dollarsalesandtranscount_daily where 
                                        cast(location_num as integer)=%s))
    order by business_date
    """
    Query=query%(store_num, store_num, store_num, store_num, store_num, store_num)

    s3_output= "s3://"+csv_bucket+"/"+athena_csv_filepath

    # Start the query to Athena
    response = xacct_client.start_query_execution(QueryString=Query, ResultConfiguration={'OutputLocation': s3_output})
    print('Please wait while inference data '+ response['QueryExecutionId']+'.csv for store '+str(store_num)+ ' is being loaded in S3....')

    time.sleep(15)
    print('Inference Data loaded in S3')

    # Loading the master daily data from S3 into memory
    s3 = boto3.client('s3')
    bucket = csv_bucket
    file_name = athena_csv_filepath+response['QueryExecutionId']+'.csv'
    obj = s3.get_object(Bucket=bucket, Key=file_name) 
    body = obj['Body']
    csv_string = body.read().decode('utf-8')
    data = pd.read_csv(StringIO(csv_string))
    data = make_it_zero(data)
    print('Inference data loaded in memory')
    print()
    pred_dt = datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
      
    # Filtering the inference data based on which inference needs to be done
    data.sort_values('business_date', inplace=True)
    print(data.shape)
    #**************************************#
        
    # Loading the model properties
    prop_fname = mp_path+str(i)+'_dollars_allproducts/'+str(i)+'_dollars_allproducts.json'
    obj = s3.get_object(Bucket= bucket_mp, Key= prop_fname)
    model_properties = json.loads(obj['Body'].read().decode('utf-8'))
    current_dt = model_properties['execution_date']
    #**************************************# 517_dollars_allproducts/517_dollars_allproducts.json
    
    # Loading the model.h5 file
    
    # model_fname = 'forecast/lstm/model-object/'+str(i)+'_dollars_allproducts/'+str(i)+'_dollars_allproducts.h5'
    model_fname = h5_path+str(i)+'_dollars_allproducts/'+str(48)+'_dollars_allproducts.h5'
    result = s3.download_file(bucket_m, model_fname, '/tmp/'+str(i)+'_dollars_allproducts.h5')
    multi_model = load_model('/tmp/'+str(i)+'_dollars_allproducts.h5')
    #**************************************#
    
    # Quality Check
    # assert(data.shape[0] == model_properties['lookback'] + model_properties['horizon'])
    
    # Scaler Implementation
    median_list = model_properties['median_list']
    median_list = [float(x) for x in median_list]
    iqr_list = model_properties['iqr_list']
    iqr_list = [float(x) for x in iqr_list]
    np.random.seed(seed=786)
    data['business_date_backUp'] = data['business_date']
    data = data.set_index('business_date')
    feat_df = data[model_properties['features_used_in_model']]
    Reqvalues = feat_df.values
    Reqvalues = Reqvalues.astype('float32')
    scaled = robust_scaler_transform(Reqvalues[:, :], median_list, iqr_list)
    #**************************************#
    
    # Converting the time series to supervised frame
    features = np.shape(Reqvalues)[1]
    lookback = model_properties['lookback']
    horizon = model_properties['horizon']
    reframed = series_to_supervised(scaled, lookback, horizon)
    reference_Var1 = lookback*features + horizon*features - 1
    Temp = [x*features  for x in list(range(lookback,lookback+horizon-1))]
    reframed.drop(reframed.columns[Temp], axis=1, inplace=True)
    
    # Below snippet will make var1(t+n) the last column in the dataframe
    y_column = 'var1(t+'+str(horizon-1)+')'
    col_list = reframed.columns.tolist()
    col_list.remove(y_column)
    col_list.extend([y_column])
    reframed = reframed[col_list]
    #**************************************#

    # Data / Tensor Preperation
    Newvalues = reframed.values
    test = Newvalues[:, :]
    
    # Split into input and outputs
    test_X = test[:, :-1]
    
    # Reshape input to be 3D [samples, timesteps, features]
    test_X = test_X.reshape((test_X.shape[0], 1, test_X.shape[1]))
    predictDates = data.tail(len(test_X)).index
    print(test_X.shape)
    yhat = multi_model.predict(test_X)
    test_X = test_X.reshape((test_X.shape[0], test_X.shape[2]))
    
    # Invert scaling for forecast
    inv_yhat = concatenate((yhat, test_X[:, 1:features]), axis=1)
    inv_yhat = robust_scaler_inverse_transform(inv_yhat, median_list, iqr_list)
    inv_yhat = inv_yhat[0][0]
    #**************************************#

    # Saving the predictions
    df = pd.DataFrame(columns=['location_num', 'businessdate', 'forecast'])
    df.loc[0,'forecast'] = inv_yhat
    df.loc[0,'businessdate'] = predictDates[0]
    df.loc[0,'location_num'] = str(i)
    df = df[['location_num', 'businessdate', 'forecast']]
    pred_fname = str(i)+'_dollars_allproducts_'+pred_dt+'.csv'
    df.to_csv('/tmp/'+pred_fname, index=False)
    transfer = S3Transfer(s3)
    target_dir = '/tmp/'
    transfer.upload_file(os.path.join(target_dir, pred_fname), upload_bucket, upload_path+pred_fname)
    #**************************************#
    
    print()
    print('**************************************************')
    print()
    print('Store Number: '+str(i)+'; Date: '+str(predictDates[0])+' Sales Forecast: $'+str(round(inv_yhat,2)))
    print('Forecasts have been saved at location:')
    print('----->  s3://'+upload_bucket+'/'+upload_path+pred_fname)
    print()

    del multi_model
    gc.collect()
    K.clear_session()
    
    # Deleting the master data from S3 bucket
    # response1 = s3.delete_object(Bucket=bucket, Key=file_name)

    # Deleting the meta data from the s3 bucket
    # response2 = s3.delete_object(Bucket=bucket, Key=file_name+".metadata")
    print('************************** End of Code *****************************')
    return 200

if __name__=="__main__":
    sales_inference(517,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path)
