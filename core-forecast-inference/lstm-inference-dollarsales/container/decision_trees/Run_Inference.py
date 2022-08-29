# coding: utf-8

# In[1]:


# Packages Needed

# Machine Learning Packages
import pandas as pd
import numpy as np
#from sklearn.preprocessing import MinMaxScaler, RobustScaler
#from sklearn.externals import joblib
from numpy import concatenate
#from sklearn.preprocessing import LabelEncoder
from keras.models import load_model

# File Handling
import boto3
from boto3.s3.transfer import S3Transfer
from io import StringIO
import os
import json
import sys
#import pickle

# Calendar / Time computation packages
import time
from datetime import datetime, date, timedelta

# Garbage Collector
import gc
secret = None 
from keras import backend as K
import tensorflow as tf
graph = tf.compat.v1.get_default_graph()
#global graph
from pytz import timezone
print("Inference test for 2 weeksout")


# In[2]:


class Inference():
    def __init__(self):
        print("class init")
        with open('config_dollarsales_inference.json') as f:
            self.config=json.load(f)
    def make_it_zero(self,DataFrame):
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
        DataFrame.loc[DataFrame['sunday'] == 1,self.config["metric"]] = 0
        DataFrame.loc[DataFrame['holiday'] == 1,self.config["metric"]] = 0
        return DataFrame
    
    
    def new_outlier_impute(self,df, colname,lookback_outlier):
    
        df = df.sort_index()
        df_rel = df.head(lookback_outlier).copy()
        non_zero = df_rel[(df_rel['dayofweek']!=7) & (df_rel['aroundchristmas']==0) &\
                         (df_rel['onedaypriorchristmas_and_new_year']==0) &\
                         (df_rel['blackfridaycheck']==0) & \
                         (df_rel['holiday']==0) & \
                         (df_rel['aroundthanksgiving']==0) & \
                         (df_rel['federalholiday']==0) & \
                         ~((df_rel['dayofweek']==6)&(df_rel['location_num'].isin([652,357,517])))]

        
        
        if(not non_zero[non_zero[colname]>0.001].empty):
            q1 = np.percentile(np.array(non_zero[colname]), 25)
            q3 = np.percentile(np.array(non_zero[colname]), 75)
            iqr = q3 - q1
            #print("Q1,Q3, IQR",q1,q3,iqr)
            #print('lower cutoff', q1-1.5*iqr)
            #print('upper cutoff', q3+1.5*iqr)
            
            
            df['outliers'] = ((df[colname]<q1-1.5*iqr)|(df[colname]>q3+1.5*iqr))&(df.index.isin(non_zero.index))
            
            non_zero['outliers']=((non_zero[colname]<q1-1.5*iqr)|(non_zero[colname]>q3+1.5*iqr))
            
            num_outliers=len(df[df['outliers']==True])
           
            
            
            if(not df[df.outliers==True].empty):
                
                dayofweek_median = non_zero[non_zero['outliers']==False].groupby(by='dayofweek')[colname].median()  
                #print(dayofweek_median)
                
                df = df.assign(dayofweek_median_col=df['dayofweek'].map(dayofweek_median))
                #print("Before",df.loc[df.outliers==True,colname])
                #print("After",df[df.outliers==True]['dayofweek_median_col'])
                df.loc[df.outliers==True,colname] = df[df.outliers==True]['dayofweek_median_col'] 
                
                df[colname].fillna(non_zero[non_zero['outliers']==False][colname].median(),inplace=True)
                df.drop(['outliers','dayofweek_median_col','location_num'], axis=1, inplace=True)
                
                return df
            
            else:
                #print("No outliers were present")
                df.drop(['outliers','location_num'], axis=1, inplace=True)
                return df
                
        else: 
            num_outliers = 0
            #print("Dataframe is empty, nothing to impute")
            df.drop(['location_num'], axis=1, inplace=True)
            return df
        
    def new_outlier_impute_dayofweek(self,df, colname,lookback_outlier):
        outlier_record = {"location_num": [], "product": [], "business_date": [], "dayofweek": [], "median": [], "lower_limit": [], "upper_limit": [], "actuals": [], "imputed_actuals": []}
        df = df.sort_index()
        df_rel = df.head(lookback_outlier).copy()
        non_zero = df_rel[(df_rel['dayofweek']!=7) & (df_rel['aroundchristmas']==0) &\
                         (df_rel['onedaypriorchristmas_and_new_year']==0) &\
                         (df_rel['blackfridaycheck']==0) & \
                         (df_rel['holiday']==0) & \
                         (df_rel['aroundthanksgiving']==0) & \
                         (df_rel['federalholiday']==0)  & \
                         ~((df_rel['dayofweek']==6)&(df_rel['location_num'].isin([652,357,517])))]



        if(not non_zero[non_zero[colname]>0.001].empty):
#        if True:
            q1_col = non_zero.groupby(by='dayofweek')[colname].quantile(0.25)
            q3_col = non_zero.groupby(by='dayofweek')[colname].quantile(0.75)
#            print("q1_col")
#            print(q1_col)
#            print("q3_col")
#            print(q3_col)
            lower_limit_col = q1_col - 1.5*(q3_col - q1_col)
            upper_limit_col = q3_col + 1.5*(q3_col - q1_col)
#            print("lower_limit_col")
#            print(lower_limit_col) 
#            print("upper_limit_col")
#            print(upper_limit_col) 
            df = df.assign(lower_limit = df['dayofweek'].map(lower_limit_col))
            df = df.assign(upper_limit = df['dayofweek'].map(upper_limit_col))
#            print(df.columns)
#            print(df.loc[:,"upper_limit"])
            df['outliers'] = ((df[colname]<df['lower_limit'])|(df[colname]>df['upper_limit']))&(df.index.isin(non_zero.index))
            non_zero = non_zero.assign(lower_limit = non_zero['dayofweek'].map(lower_limit_col))
            non_zero = non_zero.assign(upper_limit = non_zero['dayofweek'].map(upper_limit_col))
        
            non_zero['outliers']=((non_zero[colname]<non_zero['lower_limit'])|(non_zero[colname]>non_zero['upper_limit']))

            num_outliers=len(df[df['outliers']==True])

            dayofweek_median = non_zero[non_zero['outliers']==False].groupby(by='dayofweek')[colname].median()
#            print(dayofweek_median)

#            print("num_outliers: {}".format(num_outliers))
            if(not non_zero[non_zero.outliers==True].empty):

                dayofweek_median = non_zero[non_zero['outliers']==False].groupby(by='dayofweek')[colname].median()
#                print(dayofweek_median)

                df = df.assign(dayofweek_median_col=df['dayofweek'].map(dayofweek_median)) 
#                print("df dayofweek_median")
#                print(df.loc[:,"dayofweek_median_col"])
                #print("Before",df.loc[df.outliers==True,colname])
                #print("After",df[df.outliers==True]['dayofweek_median_col'])
                records_df = df.loc[df['outliers']==True, ['location_num', 'dayofweek', 'dayofweek_median_col', colname, 'dayofweek_median_col', 'lower_limit', 'upper_limit']]
#                records_df.loc[:,"business_date"] = records_df.index
                records_df.reset_index(inplace=True)
#                print(records_df) 
                outlier_record["location_num"]=list(records_df.loc[:,"location_num"].values)
                outlier_record["product"]=[999999]*len(records_df.loc[:,"location_num"].values)
                outlier_record["business_date"]=list(records_df.loc[:,"business_date"].values)
                outlier_record["dayofweek"]=list(records_df.loc[:,"dayofweek"].values)
                outlier_record["median"]=list(records_df.loc[:,"dayofweek_median_col"].values[:,0])
                outlier_record["lower_limit"]=list(records_df.loc[:,"lower_limit"].values)
                outlier_record["upper_limit"]=list(records_df.loc[:,"upper_limit"].values)
                outlier_record["actuals"]=list(records_df.loc[:,colname].values)
                outlier_record["imputed_actuals"]=list(records_df.loc[:,"dayofweek_median_col"].values[:,0])               
#                print(type(outlier_record["median"]))                
#                print(outlier_record)
                outlier_df=pd.DataFrame(data=outlier_record, columns=["location_num", "product", "business_date", "dayofweek", "median", "lower_limit", "upper_limit", "actuals", "imputed_actuals"])
                pred_fname = str(outlier_record["location_num"][0])+"_outlier_record.csv" 
                outlier_df.to_csv(pred_fname, index=False)
                df.loc[df.outliers==True,colname] = df[df.outliers==True]['dayofweek_median_col']
                s3 = boto3.client('s3')
                transfer = S3Transfer(s3)
                #target_dir = '/tmp/'
                transfer.upload_file(pred_fname, str(self.config["bucket"]), "outliers_record/dollars/"+pred_fname)

                df[colname].fillna(non_zero[non_zero['outliers']==False][colname].median(),inplace=True)
#                records_df = df[df['outliers']==True, ['location_num', 'business_date', 'dayofweek', 'dayofweek_median_col', colname, 'dayofweek_median_col']]
                df.drop(['outliers','dayofweek_median_col', 'lower_limit', 'upper_limit'], axis=1, inplace=True)

                return df

            else:
                #print("No outliers were present")
                df.drop(['outliers','location_num'], axis=1, inplace=True)
                return df

        else:
            num_outliers = 0
            #print("Dataframe is empty, nothing to impute")
            df.drop(['location_num'], axis=1, inplace=True)
            return df

    def series_to_supervised(self, data, n_in=1, n_out=1, dropnan=True):
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

    def robust_scaler_transform(self, unscaled_arr, med_list, iqr_list):
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

    def robust_scaler_inverse_transform(self, yhat, med_list, iqr_list):
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



    def nthDayPrediction(self, store_num, PredictedData, nthStartDayDate, nthEndDayDate,firstDayDate,Pacific):
            nthStartDayDateStr = nthStartDayDate.date().strftime('%Y-%m-%d')
            nthEndDayDateStr = nthEndDayDate.date().strftime('%Y-%m-%d')
            # Loading the master daily data from S3 into memory
            
            nthDayDollarsales = PredictedData.loc[nthStartDayDateStr: nthEndDayDateStr, self.config["metric"]]

            # Saving the predictions
            df = pd.DataFrame(columns=['location_num', 'businessdate', 'forecast','generation_date'], \
                  data= {'location_num':str(store_num),
                         'businessdate':nthDayDollarsales.index,
                         'forecast': nthDayDollarsales.values,
                         'generation_date':datetime.strftime(datetime.now(tz=Pacific),'%Y-%m-%d') })

            df['forecast'] = df['forecast'].apply(lambda x: 0 if x<0 else x)
            
            nDays = nthStartDayDate - firstDayDate
            nDays = nthStartDayDate - firstDayDate
            bucket=str(self.config["bucket"])
            if nDays.days >= int(self.config["longer_forecast_start"]):
                if nDays.days <= int(self.config["longer_forecast_end"]):
                    pathToSave = str(self.config["base_s3_path"])+str(self.config["longer_folder_name"])+'/location_num='+str(store_num)+'/' 
                    pred_fname = str(self.config["longer_filename"])+str(store_num)+'.csv'
                    df.to_csv('/tmp/'+pred_fname, index=False)
                    s3 = boto3.client('s3')
                    transfer = S3Transfer(s3)
                    target_dir = '/tmp/'
                    transfer.upload_file(os.path.join(target_dir, pred_fname), bucket, pathToSave+pred_fname)
                    os.remove(os.path.join(target_dir, pred_fname)) 
                else:
                    pathToSave = str(self.config["base_s3_path"])+str(self.config["redundancy_folder_name"])+'/location_num='+str(store_num)+'/' 
                    pred_fname = str(self.config["redundancy_filename"])+str(store_num)+'.csv'
                    df.to_csv('/tmp/'+pred_fname, index=False)
                    s3 = boto3.client('s3')
                    transfer = S3Transfer(s3)
                    target_dir = '/tmp/'
                    transfer.upload_file(os.path.join(target_dir, pred_fname), bucket, pathToSave+pred_fname)
                    os.remove(os.path.join(target_dir, pred_fname)) 

            else:
                pathToSave = str(self.config["base_s3_path"])+str(self.config["shorter_folder_name"])+'/location_num='+str(store_num)+'/' 
                pred_fname = str(self.config["shorter_filename"])+str(store_num)+'.csv'
                df.to_csv('/tmp/'+pred_fname, index=False)
                s3 = boto3.client('s3')
                transfer = S3Transfer(s3)
                target_dir = '/tmp/'
                transfer.upload_file(os.path.join(target_dir, pred_fname), bucket, pathToSave+pred_fname)
                os.remove(os.path.join(target_dir, pred_fname))

    
    def ZeroImputing(self,data, metric):
        df=data
        df_rel=df.head(56)
        NonZero = df_rel[(df_rel['dayofweek'] != 7) & (df_rel['holiday']==0)]
        Median_dayofweek=NonZero[NonZero[metric]>0.001].groupby(by='dayofweek')[metric].median()    
        df=df.assign(median_dayofweek=df['dayofweek'].map(Median_dayofweek))
        df['outliers'] = (df[metric] < 0.001) & df.index.isin(NonZero.index) 
        df.loc[df.outliers==True,metric] = df[df.outliers==True]['median_dayofweek'] 
        #data=df.drop(columns=['outliers','median_dayofweek'])
        del(df['outliers'])
        del(df['median_dayofweek'])
        data=df
        data = data.fillna(0)
        return data 

    def MakeHolidayForecastZero(self,ForecastedData):
        ForecastedData.loc[ForecastedData['holiday'] == 1,'trans_count'] = 0
        return ForecastedData
    
    def sales_inference(self, store_num,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path):
        '''
        In this function, a location_number is passed as paramter to return its sales prediction for n days ahead.
        The prediction for nth day is saved at the s3 path. The prediction output format is location_num, businessdate, forcast
        Parameters
        ----------
        - store_num: Location_num of the store
        '''
        #store_num = '20'
        NumberDaysToPredict = int(self.config["redundancy_forecast_end"])+2
        i = str(store_num).lstrip('0')

        # Making connection to Athena
        s3_output = 's3://prod-q-forecasting-artifacts/forecast/lstm/temp_files/'
        Pacific = timezone('US/Pacific')
        
        # Loading the master daily data from S3 into memory
        s3 = boto3.client('s3')
        bucket = str(self.config["bucket"]) 
        metric=str(self.config["metric"])
        file_name = str(self.config["data_key"])+'location_num=' + str(store_num) + '/Inference_daily' + '.csv'
        obj = s3.get_object(Bucket=bucket, Key=file_name) 
        body = obj['Body']
        csv_string = body.read().decode('utf-8')
        data = pd.read_csv(StringIO(csv_string))


        data = self.make_it_zero(data)
        data = self.ZeroImputing(data,metric=metric)
        
        # hot encode federal holiday features
        data = pd.get_dummies(data, columns=['federalholiday_name'])        
        
        print('Inference data loaded in memory')
        pred_dt = datetime.today().strftime('%Y-%m-%d')

        # Filtering the inference data based on which inference needs to be done
        data.sort_values('business_date', inplace=True)
        #**************************************#

        # Loading the model properties
        bucket_mp = bucket
        prop_fname = 'model-object-retraining-jan21/'+str(i)+'_dollars_allproducts/'+str(i)+'_dollars_allproducts.json'
        print(prop_fname)
        obj = s3.get_object(Bucket= bucket_mp, Key= prop_fname)
        model_properties = json.loads(obj['Body'].read().decode('utf-8'))
        current_dt = model_properties['execution_date']
        #**************************************#
        
        for col in model_properties['features_used_in_model']:
            if col not in data:
                data[col] = 0

        # Loading the model.h5 file
        bucket_m = bucket
        model_fname = 'model-object-retraining-jan21/'+str(i)+'_dollars_allproducts/'+str(i)+'_dollars_allproducts.h5'
        print(model_fname)
        result = s3.download_file(bucket_m, model_fname, '/tmp/'+str(i)+'_dollars_allproducts.h5')
        multi_model = load_model('/tmp/'+str(i)+'_dollars_allproducts.h5')
        os.remove('/tmp/'+str(i)+'_dollars_allproducts.h5')
        #**************************************# 

        # Quality Check
        #assert(data.shape[0] == model_properties['lookback'] + model_properties['horizon'])

        # Scaler Implementation
        median_list = model_properties['median_list']
        lookback = model_properties['lookback']
        horizon = model_properties['horizon']
        median_list = [float(x) for x in median_list]
        iqr_list = model_properties['iqr_list']
        iqr_list = [float(x) for x in iqr_list]
        np.random.seed(seed=786)

        data['business_date_backUp'] = data['business_date']
        data = data.set_index('business_date')
        #data.to_csv("/home/ubuntu/outlier/data_new.csv")
        #print("Shape of Data", data.shape)
        data['location_num']=store_num
        colname = metric
        #data = self.new_outlier_impute(data, colname,lookback_outlier=25)
        data = self.new_outlier_impute_dayofweek(data, colname,lookback_outlier=56)
        
        ##################### REMODELING - REPLACE WITH HISTORY ###############################
        middle_days = len(data.index) - 88
        if middle_days>0:
            #print("middle days: ", len(data.index) - 88)

            #print(data.index.max())
            #print(type(data.index.max()))
            target_weekday = data[data.index==datetime.strftime(datetime.strptime(data.index.max(), "%Y-%m-%d")+timedelta(days=-32), "%Y-%m-%d")]["dayofweek"].values[0]
            #print(target_weekday)

            history_data = data.head(56)
            history_end_date = history_data[history_data["dayofweek"]==target_weekday].index.max()
            if middle_days>=25:
                history_start_date = datetime.strftime(datetime.strptime(history_end_date, "%Y-%m-%d")+timedelta(days=-24), "%Y-%m-%d")
            else:
                history_start_date = datetime.strftime(datetime.strptime(history_end_date, "%Y-%m-%d")+timedelta(days=-middle_days+1), "%Y-%m-%d")
            #print(history_start_date, history_end_date)        
            req_data = history_data.loc[history_start_date:history_end_date, self.config["metric"]]
            #print(req_data)
            inf_end_date = datetime.strftime(datetime.strptime(data.index.max(), "%Y-%m-%d")+timedelta(days=-32), "%Y-%m-%d")
            if middle_days>=25:
                inf_start_date = datetime.strftime(datetime.strptime(inf_end_date, "%Y-%m-%d")+timedelta(days=-24), "%Y-%m-%d")
            else:
                inf_start_date = datetime.strftime(datetime.strptime(inf_end_date, "%Y-%m-%d")+timedelta(days=-middle_days+1), "%Y-%m-%d")
            #print(inf_start_date)
            data.loc[inf_start_date:inf_end_date,self.config["metric"]] = req_data.values
        ######################################################################################
        
        Tempdate = data.tail(NumberDaysToPredict).index[0]
        #print (Tempdate)
        modifiedDate = datetime.strptime(Tempdate, "%Y-%m-%d")
        lookback=int(self.config["lookback"])
        horizon=int(self.config["horizon"])
        Lastdate = modifiedDate + timedelta(days=0)
        Firstdate = modifiedDate + timedelta(days=-(lookback+horizon-1))

        Firstdate = Firstdate.date().strftime('%Y-%m-%d')
        Lastdate = Lastdate.date().strftime('%Y-%m-%d')
        
        for ithDay in range(1,NumberDaysToPredict+1):
            TempData = data.loc[Firstdate:Lastdate, model_properties['features_used_in_model']]
            #print ('Lastdate = ', Lastdate, 'Firstdate = ', Firstdate)
            
            # temporary fix to fill NaN federal holiday features with 0, to be removed once schema is changed
            for col in list(TempData.columns.values):
                if col.startswith('federalholiday_name'):
                    TempData[col].fillna(0, inplace=True)            

            if TempData.loc[TempData.index.max(),'dayofweek'] == 7 or (TempData.loc[TempData.index.max(),'dayofweek'] == 6                                                                    and i in ['517','375', '652']):
                inv_yhat = 0
            else:

                feat_df = TempData[model_properties['features_used_in_model']]
                Reqvalues = feat_df.values

                Reqvalues = Reqvalues.astype('float32')
                scaled = self.robust_scaler_transform(Reqvalues[:, :], median_list, iqr_list)
                #**************************************#

                # Converting the time series to supervised frame
                features = np.shape(Reqvalues)[1]
                reframed = self.series_to_supervised(scaled, lookback, horizon)
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
                yhat = multi_model.predict(test_X)
                test_X = test_X.reshape((test_X.shape[0], test_X.shape[2]))

                # Invert scaling for forecast
                inv_yhat = concatenate((yhat, test_X[:, 1:features]), axis=1)
                inv_yhat = self.robust_scaler_inverse_transform(inv_yhat, median_list, iqr_list)
                inv_yhat = inv_yhat[0][0]
                #print (inv_yhat)

            data.loc[TempData.index.max(),metric] = inv_yhat
            if NumberDaysToPredict-ithDay == 0:
                break
            Lastdate = data.tail(NumberDaysToPredict-ithDay).index[0]
            modifiedDate = datetime.strptime(data.tail(NumberDaysToPredict-ithDay).index[0], "%Y-%m-%d")
            Firstdate = modifiedDate + timedelta(days=-(lookback+horizon-1))
            Firstdate = Firstdate.date().strftime('%Y-%m-%d')

        tendayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=int(self.config["shorter_forecast_start"]))
        fourteendayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=int(self.config["shorter_forecast_end"]))
        thirtydayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=int(self.config["longer_forecast_start"]))
        thirtyfourdayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=int(self.config["longer_forecast_end"]))
        redundancyAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=int(self.config["redundancy_forecast_start"]))
        redundancyfourdayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=int(self.config["redundancy_forecast_end"]))

        
        # tendayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=10)
        # fourteendayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=13)
        # thirtydayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=22)
        # thirtyfourdayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=25)
        # thirtyfiveAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=26)
        # fourtydayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=30)
        
        data = self.MakeHolidayForecastZero(data)
        
        self.nthDayPrediction(store_num, data, tendayAhead, fourteendayAhead,datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d"),Pacific)
 
        self.nthDayPrediction(store_num, data, thirtydayAhead,thirtyfourdayAhead,datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d"),Pacific)

        self.nthDayPrediction(store_num, data, redundancyAhead, redundancyfourdayAhead,datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d"),Pacific)
 

        # pred_fname = str(store_num)+'_sales_inference'+'.csv'
        # df = pd.DataFrame(columns=['location_num','product','model_type'],data={'location_num':store_num,'product':'sales','model_type':'LSTM'},index=[0])
        # df.to_csv('/tmp/'+pred_fname, index=False)
        # s3 = boto3.client('s3')
        # transfer = S3Transfer(s3)
        # target_dir = '/tmp/'
        # transfer.upload_file(os.path.join(target_dir, pred_fname), 'q-forecasting-artifacts',  'inference_completed/Location_num='+str(store_num)+'/'+pred_fname)
 
        del multi_model
        gc.collect()
        try:
            K.clear_session()
            tf.reset_default_graph()
        except:
            print ('tf graph session cleared')

    #**************************************End**************************************#

    def predict(self,store_num,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path):
        with graph.as_default():
            
            return self.sales_inference(store_num,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path)


# In[3]:


Infer=Inference()

if __name__=="__main__":
    store_num =  sys.argv[1]
    bucket_name = ''#sys.argv[2]
    athena_csv_filepath = '' #sys.argv[3]
    mp_path = '' #sys.argv[4]
    h5_path = '' #sys.argv[5]
    upload_path = '' #sys.argv[6]
    #Infer = Inference()
    #Infer.predict(store_num,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path)
    Infer.sales_inference(store_num,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path)