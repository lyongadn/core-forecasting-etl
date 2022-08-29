# Packages Needed

# Machine Learning Packages
# Machine Learning Packages
import pandas as pd
import numpy as np
#from sklearn.preprocessing import MinMaxScaler, RobustScaler
#from sklearn.externals import joblib
from numpy import concatenate
#from sklearn.preprocessing import LabelEncoder
from keras.models import load_model
import keras

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

import glob
import shutil
from pytz import timezone

from botocore.config import Config
config = Config(
    retries = {
        'max_attempts' : 10,
        'mode' : 'standard'
    }
)

# In[2]:

class Inference():
    def __init__(self):

        print("class init")
        with open('config_itemcount_inference.json') as f:
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
        #DataFrame.loc[DataFrame['sunday'] == 1,'sum_daily_quantity'] = 0
        #DataFrame.loc[DataFrame['holiday'] == 1,'sum_daily_quantity'] = 0
        return DataFrame


    def new_outlier_impute(self,df, colname,lookback_outlier):
        """
        Check for outliers in the data once in sales inference. 
        Imputatation (by clean data (except for any holidays & no sundays & saturdays for 652, 357, 517) only 
        Parameters
        ----------
            - DataFrame: Master Dataframe
            - colname: metric to impute (sales_sub_total, sum_daily_quantity, transaction_count)
            - lookback_outlier: no of rows to compute the imputation Quartile values

        Returns: 
        ----------
        - DataFrame: with outliers imputed in the main dataframe

        """

        df = df.sort_index()
        df_rel = df.head(lookback_outlier).copy()
        non_zero = df_rel[(df_rel['dayofweek']!=7) & (df_rel['aroundchristmas']==0) & \
                            (df_rel['onedaypriorchristmas_and_new_year']==0) & \
                            (df_rel['blackfridaycheck']==0) &  \
                            (df_rel['holiday']==0) &\
                            (df_rel['aroundthanksgiving']==0) &  \
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
                
                
                #updated method to impute periodic outliers
                dayofweek_median_outliers = non_zero.groupby(by='dayofweek')[colname].median()  
                df = df.assign(dayofweek_median_col_outliers=df['dayofweek'].map(dayofweek_median_outliers))
                df.loc[(pd.isnull(df[colname])),colname] = df[(pd.isnull(df[colname]))]['dayofweek_median_col_outliers'] 
                
                
                #old method to impute periodic outliers
                #df[colname].fillna(non_zero[non_zero['outliers']==False][colname].median(),inplace=True)


                return df

            else:
                #print("No outliers were present")
                #df.drop(['outliers','location_num'], axis=1, inplace=True)
                return df

        else: 
            num_outliers = 0
            #print("Dataframe is empty, nothing to impute")
            #df.drop(['location_num'], axis=1, inplace=True)
            return df
        
    def new_outlier_impute_dayofweek(self,df, colname,lookback_outlier):
        outlier_record = {"location_num": [], "product": [], "business_date": [], "dayofweek": [], "median": [], "lower_limit": [], "upper_limit": [], "actuals": [], "imputed_actuals": [], "generation_date": []}
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
                records_df = df.loc[df['outliers']==True, ['location_num', 'dayofweek', 'dayofweek_median_col', colname, 'dayofweek_median_col', 'lower_limit', 'upper_limit', 'product']]
#                records_df.loc[:,"business_date"] = records_df.index
                records_df.reset_index(inplace=True)
#                print(records_df) 
                outlier_record["location_num"]=list(records_df.loc[:,"location_num"].values)
                outlier_record["product"]=list(records_df.loc[:,"product"].values)
                outlier_record["business_date"]=list(records_df.loc[:,"business_date"].values)
                outlier_record["dayofweek"]=list(records_df.loc[:,"dayofweek"].values)
                outlier_record["median"]=list(records_df.loc[:,"dayofweek_median_col"].values[:,0])
                outlier_record["lower_limit"]=list(records_df.loc[:,"lower_limit"].values)
                outlier_record["upper_limit"]=list(records_df.loc[:,"upper_limit"].values)
                outlier_record["actuals"]=list(records_df.loc[:,colname].values)
                outlier_record["imputed_actuals"]=list(records_df.loc[:,"dayofweek_median_col"].values[:,0])   
                
                outlier_record["generation_date"]=[datetime.now().strftime("%Y-%m-%d")]*len(records_df.loc[:,"location_num"].values)
#               
                outlier_df=pd.DataFrame(data=outlier_record, columns=["location_num", "product", "business_date", "dayofweek", "median", "lower_limit", "upper_limit", "actuals", "imputed_actuals", "generation_date"])
                pred_fname = str(outlier_record["location_num"][0])+"_outlier_record_"+str(outlier_record["product"][0])+".csv" 
                outlier_df.to_csv(pred_fname, index=False)
                df.loc[df.outliers==True,colname] = df[df.outliers==True]['dayofweek_median_col']
                s3 = boto3.client('s3')
                transfer = S3Transfer(s3)
                #target_dir = '/tmp/'
                transfer.upload_file(pred_fname, str(self.config["lstm_bucket"]), "outliers_record/itemcount/location_num="+str(outlier_record["location_num"][0])+"/"+pred_fname)

                df[colname].fillna(non_zero[non_zero['outliers']==False][colname].median(),inplace=True)
#                records_df = df[df['outliers']==True, ['location_num', 'business_date', 'dayofweek', 'dayofweek_median_col', colname, 'dayofweek_median_col']]
                df.drop(['outliers','dayofweek_median_col', 'lower_limit', 'upper_limit'], axis=1, inplace=True)
                os.remove(pred_fname)
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


    def nthDayPrediction(self, store_num, product_num,PredictedData, nthStartDayDate, nthEndDayDate,firstDayDate,Pacific,todayWithoutTZ):
        #store_num, product_num,data, tendayAhead, fourteendayAhead,datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d"),Pacific
        nthStartDayDateStr = nthStartDayDate.date().strftime('%Y-%m-%d')
        nthEndDayDateStr = nthEndDayDate.date().strftime('%Y-%m-%d')
        # Loading the master daily data from S3 into memory
        
        nthDaySales = PredictedData.loc[nthStartDayDateStr: nthEndDayDateStr, self.config["metric"]]
        
        
        nDays = nthStartDayDate - firstDayDate
        
        if nDays.days >= int(self.config["longer_forecast_start"]):
            if nDays.days <= int(self.config["longer_forecast_end"]):
                # Saving the predictions
                df = pd.DataFrame(columns=['location_num', 'product', 'businessdate', 'forecast','generation_date'], \
                data= {'location_num':str(store_num),
                     'product':str(product_num),
                     'businessdate':nthDaySales.index,
                     #'businessdate':pd.date_range(datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=21),periods=4),
                     'forecast': nthDaySales.values,
                     'generation_date':datetime.strftime(datetime.now(tz=Pacific),'%Y-%m-%d')})
        
                df['forecast'] = df['forecast'].apply(lambda x: 0 if x<0 else x)
                #pathToSave = 'forecast/itemcount/daily/30days-ahead/location_num='+str(store_num)+'/'
#                 pred_fname = 'thirtydayAhead_'+str(product_num)+'.csv'
#                 print (pred_fname)
#                 s3 = boto3.client('s3')
#                 transfer = S3Transfer(s3)
                
#                 if not os.path.exists('/tmp/thirty/'): # ./temp/
#                         os.makedirs('/tmp/thirty/')

#                 df.to_csv(pred_fname, index=False)
    
                pred_fname = str(store_num)+'_'+str(self.config["longer_filename"])+'_'+str(product_num)+'.csv'
                print (pred_fname)
                s3 = boto3.client('s3')
                transfer = S3Transfer(s3)
                
                if not os.path.exists('/tmp/'+str(self.config["longer_local_name"])+'/'): # ./temp/
                        os.makedirs('/tmp/'+str(self.config["longer_local_name"])+'/')

                df.to_csv('/tmp/'+str(self.config["longer_local_name"])+'/'+pred_fname, index=False)

                # pathToSave = "forecast/itemcount/daily/"+'30thday/location_num='+str(store_num)+'/'
                # transfer.upload_file('/tmp/'+pred_fname, 'test-q-forecasting-artifacts', pathToSave+pred_fname)
                # os.remove('/tmp/'+pred_fname)

            else:
                # Saving the predictions
                df = pd.DataFrame(columns=['location_num', 'product', 'businessdate', 'forecast','generation_date'], \
                      data= {'location_num':str(store_num),
                             'product':str(product_num),
                             'businessdate':nthDaySales.index,
                             #'businessdate':pd.date_range(datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=25),periods=5),
                             'forecast': nthDaySales.values,
                            'generation_date':datetime.strftime(datetime.now(tz=Pacific),'%Y-%m-%d')})

                df['forecast'] = df['forecast'].apply(lambda x: 0 if x<0 else x)
                #pathToSave = 'forecast/itemcount/daily/redundancy/location_num='+str(store_num)+'/' 
#                 pred_fname = 'thirtyfourAhead_'+str(product_num)+'.csv'
#                 print (pred_fname)
# #                 s3 = boto3.client('s3')
# #                 transfer = S3Transfer(s3)

# #                 if not os.path.exists('/tmp/thirtyfour/'):
# #                         os.makedirs('/tmp/thirtyfour/')

#                 df.to_csv(pred_fname, index=False)
                pred_fname = str(store_num)+'_'+str(self.config["redundancy_filename"])+'_'+str(product_num)+'.csv'
                print (pred_fname)
                s3 = boto3.client('s3')
                transfer = S3Transfer(s3)
                
                if not os.path.exists('/tmp/'+str(self.config["redundancy_local_name"])+'/'): # ./temp/
                        os.makedirs('/tmp/'+str(self.config["redundancy_local_name"])+'/')

                df.to_csv('/tmp/'+str(self.config["redundancy_local_name"])+'/'+pred_fname, index=False)
                # pathToSave = "forecast/itemcount/daily/"+'redundancy/location_num='+str(store_num)+'/'
                # transfer.upload_file('/tmp/'+pred_fname, 'test-q-forecasting-artifacts', pathToSave+pred_fname)
                # os.remove('/tmp/'+pred_fname)


        else:
            # Saving the predictions
            df = pd.DataFrame(columns=['location_num', 'product', 'businessdate', 'forecast','generation_date'], \
                  data= {'location_num':str(store_num),
                         'product':str(product_num),
                         'businessdate':nthDaySales.index,
                         #'businessdate':pd.date_range(datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=9),periods=4),
                         'forecast': nthDaySales.values,
                        'generation_date':datetime.strftime(datetime.now(tz=Pacific),'%Y-%m-%d')})

            df['forecast'] = df['forecast'].apply(lambda x: 0 if x<0 else x)
            #pathToSave = 'forecast/itemcount/daily/10days-ahead/location_num='+str(store_num)+'/'
#             pred_fname = 'tendayAhead_'+str(product_num)+'.csv'
#             print (pred_fname)
#             s3 = boto3.client('s3')
#             transfer = S3Transfer(s3)            

#             if not os.path.exists('/tmp/ten/'):
#                         os.makedirs('/tmp/ten/')
            
#             df.to_csv(pred_fname, index=False)
            pred_fname = str(store_num)+'_'+str(self.config["shorter_filename"])+'_'+str(product_num)+'.csv'
            print (pred_fname)
            s3 = boto3.client('s3')
            transfer = S3Transfer(s3)
                
            if not os.path.exists('/tmp/'+str(self.config["shorter_local_name"])+'/'): # ./temp/
                os.makedirs('/tmp/'+str(self.config["shorter_local_name"])+'/')

            df.to_csv('/tmp/'+str(self.config["shorter_local_name"])+'/'+pred_fname, index=False)
            # pathToSave = "forecast/itemcount/daily/"+'10thday/location_num='+str(store_num)+'/'
            # transfer.upload_file('/tmp/'+pred_fname, 'test-q-forecasting-artifacts', pathToSave+pred_fname)
            # os.remove('/tmp/'+pred_fname)

        
    def combine_csv(self,infiles_path="/tmp/",path_save="forecast/itemcount/daily",store_num=3262):

        s3 = boto3.client('s3')
        transfer = S3Transfer(s3)

        allFiles = glob.glob(str(infiles_path)+ str(self.config["shorter_local_name"])+"/*.csv")
        with open(str(infiles_path) + str(self.config["shorter_filename"])+'.csv', 'wb') as outfile:
            for i, fname in enumerate(allFiles):
                with open(fname, 'rb') as infile:
                    if i != 0:
                        infile.readline()  # Throw away header on all but first file
                        # Block copy rest of file from input to output without parsing
                    shutil.copyfileobj(infile, outfile)
                    #print(fname + " has been imported.")
        df_10 = pd.read_csv('/tmp/'+str(self.config["shorter_filename"])+'.csv')
        df_10.to_csv('/tmp/'+str(self.config["shorter_filename"])+'.csv',index=False,header=False)
        pathToSave = str(path_save)+str(self.config["shorter_folder_name"])+'/location_num='+str(store_num)+'/'
        pred_fname = str(self.config["shorter_filename"])+'.csv'
        transfer.upload_file(os.path.join(str(infiles_path), pred_fname), str(self.config["bucket"]),
                             pathToSave+pred_fname)
        time.sleep(0.02)
        shutil.rmtree(os.path.join(str(infiles_path),str(self.config["shorter_local_name"])))
        os.remove(os.path.join(infiles_path, pred_fname))
        


        allFiles = glob.glob(str(infiles_path)+ str(self.config["longer_local_name"])+"/*.csv")
        with open(str(infiles_path) + str(self.config["longer_filename"])+'.csv', 'wb') as outfile:
            for i, fname in enumerate(allFiles):
                with open(fname, 'rb') as infile:
                    if i != 0:
                        infile.readline()  # Throw away header on all but first file
                        # Block copy rest of file from input to output without parsing
                    shutil.copyfileobj(infile, outfile)
                    #print(fname + " has been imported.")
        df_30 = pd.read_csv('/tmp/'+str(self.config["longer_filename"])+'.csv')
        df_30.to_csv('/tmp/'+str(self.config["longer_filename"])+'.csv',index=False,header=False)
        pathToSave = str(path_save)+str(self.config["longer_folder_name"])+'/location_num='+str(store_num)+'/'
        pred_fname = str(self.config["longer_filename"])+'.csv'
        s3 = boto3.client('s3')
        transfer = S3Transfer(s3)
        transfer.upload_file(os.path.join(str(infiles_path), pred_fname), str(self.config["bucket"]),
                             pathToSave+pred_fname)
        time.sleep(0.02)
        shutil.rmtree(os.path.join(str(infiles_path),str(self.config["longer_local_name"])))
        os.remove(os.path.join(infiles_path, pred_fname))
        
        allFiles = glob.glob(str(infiles_path)+ str(self.config["redundancy_local_name"])+"/*.csv")
        with open(str(infiles_path) + str(self.config["redundancy_filename"])+'.csv', 'wb') as outfile:
            for i, fname in enumerate(allFiles):
                with open(fname, 'rb') as infile:
                    if i != 0:
                        infile.readline()  # Throw away header on all but first file
                        # Block copy rest of file from input to output without parsing
                    shutil.copyfileobj(infile, outfile)
                    #print(fname + " has been imported.")
        df_34 = pd.read_csv('/tmp/'+str(self.config["redundancy_filename"])+'.csv')
        df_34.to_csv('/tmp/'+str(self.config["redundancy_filename"])+'.csv',index=False,header=False)
        pathToSave = str(path_save)+str(self.config["redundancy_folder_name"])+'/location_num='+str(store_num)+'/'
        pred_fname = str(self.config["redundancy_filename"])+'.csv'
        s3 = boto3.client('s3')
        transfer = S3Transfer(s3)
        transfer.upload_file(os.path.join(str(infiles_path), pred_fname), str(self.config["bucket"]),
                             pathToSave+pred_fname)
        time.sleep(0.02)
        shutil.rmtree(os.path.join(str(infiles_path),str(self.config["redundancy_local_name"])))
        os.remove(os.path.join(infiles_path, pred_fname))



        
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


    def forecastZero(self,store_num,product_num,Pacific,todayWithoutTZ):

        tendayAhead = datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=self.config["shorter_forecast_start"]-1)
        fourteendayAhead = datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=self.config["shorter_forecast_end"]-1)
        thirtydayAhead = datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=self.config["longer_forecast_start"]-1)
        thirtyfourdayAhead = datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=self.config["longer_forecast_end"]-1)
        redundancyAhead = datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=self.config["redundancy_forecast_start"]-1)
        redundancyfourdayAhead = datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=self.config["redundancy_forecast_end"]-1)

        data = pd.DataFrame(columns=[str(self.config["metric"])],data={str(self.config["metric"]):np.zeros((33,))})

        data.set_index(pd.date_range(datetime.strptime(todayWithoutTZ, "%Y-%m-%d")-timedelta(days=3),\
                                          redundancyfourdayAhead),inplace=True)
        
        self.nthDayPrediction(store_num, product_num,data, \
            tendayAhead, fourteendayAhead,\
            datetime.strptime(todayWithoutTZ, "%Y-%m-%d")-timedelta(days=2),Pacific,todayWithoutTZ)

        self.nthDayPrediction(store_num, product_num,data, \
            thirtydayAhead,thirtyfourdayAhead,\
            datetime.strptime(todayWithoutTZ, "%Y-%m-%d")-timedelta(days=2),Pacific,todayWithoutTZ)

        self.nthDayPrediction(store_num, product_num,data, \
            redundancyAhead,redundancyfourdayAhead,\
            datetime.strptime(todayWithoutTZ, "%Y-%m-%d")-timedelta(days=2),Pacific,todayWithoutTZ)

    def MakeHolidayForecastZero(self,ForecastedData):
        ForecastedData.loc[ForecastedData['holiday'] == 1,str(self.config["metric"])] = 0
        return ForecastedData
    
    def sales_inference(self,index,store_num,product_num, bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path,data_df):

        '''
        In this function, a location_number is passed as paramter to return its sales prediction for n days ahead.
        The prediction for nth day is saved at the s3 path. The prediction output format is location_num, businessdate, forcast

        Parameters
        ----------
        - store_num: Location_num of the store'''
        # Since we want to forecast 32 days at once, 9-13, 21-24 and 25-29. Since data is two days behind the actual date, its 29+2. 
        # Plus 1 because of python zero indexing
        NumberDaysToPredict = int(self.config["redundancy_forecast_end"])+2

        # Sort based on business_date for obvious reasons, this is double safety. data is already sorted in query also
        data_df.sort_values('business_date', inplace=True)

        # Set indexes to business_date and keep a backup column with dates in it
        data_df['business_date_backUp'] = data_df['business_date']
        data_df = data_df.set_index('business_date')

        # Temp date is the day we need to start forecasting for, Hence take lower 32 days.
        Tempdate = data_df.tail(NumberDaysToPredict).index[0]

        # Convert to datetime format to calculate any missing dates if at all.
        modifiedDate = datetime.strptime(Tempdate, "%Y-%m-%d")
        
        #Define timeZone, Why Pacific? We will have time till 3AM ET to run our pipeline still having same date.
        Pacific = timezone('US/Pacific')
        today = datetime.now(tz=Pacific)

        todayWithoutTZ = datetime.strftime(today,'%Y-%m-%d')
        # todayWithoutTZ='2019-01-27'
        
        # max_date is difference in dates, if difference in dates is more than 20. Forecast zeros. Its not possible to run and forecast proper value
        max_date = datetime.strptime(todayWithoutTZ, "%Y-%m-%d")- modifiedDate

        if max_date.days > 6:
            #forecastZero(self,store_num,product_num,Pacific,todayWithoutTZ)
            self.forecastZero(store_num,product_num,Pacific,todayWithoutTZ)
            return
        
        dataframe = data_df
        store_num = dataframe.location_num.unique()[0]
        print ('store number', store_num)
        product_num = dataframe['product'].unique()[0]
        print ('product number', product_num)
        start = time.time()
        
        data = self.make_it_zero(dataframe)
        data = self.ZeroImputing(data,metric=str(self.config["metric"]))

        # Loading the model properties
        s3 = boto3.client('s3', config=config)
        bucket_mp = str(self.config["bucket"])
        # prop_fname = 'model-object/'+str(store_num)+'_itemcount_'+str(product_num)+'/'+str(store_num)+'_itemcount_'+str(product_num)+'.json'
        prop_fname = 'model-object/'+str(store_num).lstrip('0')+'_itemcount_'+str(product_num)+'/'+str(store_num).lstrip('0')+'_itemcount_'+str(product_num)+'.json'
        try:
            obj = s3.get_object(Bucket= bucket_mp, Key= prop_fname)
        except:
            print ('Model object Not Present for',str(store_num), 'and', str(product_num))
            return
        model_properties = json.loads(obj['Body'].read().decode('utf-8'))
#         with open('868_itemquantity_90002_np.json','r') as mp:
#             model_properties=json.load(mp)
#         model_properties=json.load('868_itemquantity_90002_np.json')
        current_dt = model_properties['execution_date']
        #**************************************#


        
        # hot encode federal holiday features
        data = pd.get_dummies(data, columns=['federalholiday_name']) 
        

        # Filtering the inference data based on which inference needs to be done
        
        #**************************************#


        # Loading the model.h5 file
        bucket_m = bucket_mp
        # model_fname = 'model-object/'+str(store_num)+'_itemcount_'+str(product_num)+'/'+str(store_num)+'_itemcount_'+str(product_num)+'.h5'
        model_fname = 'model-object/'+str(store_num).lstrip('0')+'_itemcount_'+str(product_num)+'/'+str(store_num).lstrip('0')+'_itemcount_'+str(product_num)+'.h5'

        result = s3.download_file(bucket_m, model_fname, '/tmp/'+str(store_num)+'_itemcount_'+str(product_num)+'.h5')
        print (store_num,product_num)
        multi_model = load_model('/tmp/'+str(store_num)+'_itemcount_'+str(product_num)+'.h5')
        time.sleep(0.02)
        os.remove('/tmp/'+str(store_num)+'_itemcount_'+str(product_num)+'.h5')
        #**************************************#
#         multi_model=load_model('868_itemquantity_90002_np.h5')

        # Scaler Implementation
        median_list = model_properties['median_list']
        lookback = model_properties['lookback']
        horizon = model_properties['horizon']
        median_list = [float(x) for x in median_list]
        iqr_list = model_properties['iqr_list']
        iqr_list = [float(x) for x in iqr_list]
        np.random.seed(seed=786)
        #print("Shape of Data",data.shape)

        data['location_num']=store_num
        colname = str(self.config["metric"])
        #data = self.new_outlier_impute(data, colname,25)
        
        data = self.new_outlier_impute_dayofweek(data, colname,56)
        
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
        
        lookback=int(self.config["lookback"])
        horizon=int(self.config["horizon"])
        Lastdate = modifiedDate + timedelta(days=0)
        ###############################################################
        Firstdate = modifiedDate + timedelta(days=-(lookback+horizon-1)) #please look into the number, its hard coded.
        ###############################################################
        Firstdate = Firstdate.date().strftime('%Y-%m-%d')
        Lastdate = Lastdate.date().strftime('%Y-%m-%d')
        #print("data columns are", data.columns)
        for ithDay in range(1,NumberDaysToPredict+1):
            TempData = data.loc[Firstdate:Lastdate, :]          
            TempData = TempData.reindex(columns=model_properties['features_used_in_model'])
            
            #TempData = data.loc[Firstdate:Lastdate, model_properties['features_used_in_model']]
            #print ('Lastdate = ', Lastdate, 'Firstdate = ', Firstdate)
            
            # temporary fix to fill NaN federal holiday features with 0, to be removed once schema is changed
            for col in list(TempData.columns.values):
                 if col.startswith('federalholiday_name'):
                     TempData[col].fillna(0, inplace=True)            

            # if TempData.loc[TempData.index.max(),'dayofweek'] == 7 or (TempData.loc[TempData.index.max(),'dayofweek'] == 6                                                                    and store_num in ['517','375', '652']):
            if TempData.loc[TempData.index.max(),'dayofweek'] == 7 or (TempData.loc[TempData.index.max(),'dayofweek'] == 6                                                                    and str(store_num).lstrip('0') in ['517','375', '652']):
                inv_yhat = 0
            else:
                
                feat_df = TempData[model_properties['features_used_in_model']]
                #feat_df['location_num']=store_num
                #colname = 'sum_daily_quantity'
                #feat_df2 = self.outlier_impute(feat_df, colname,lookback)
                Reqvalues = feat_df.values
                Reqvalues = Reqvalues.astype('float32')
                scaled = self.robust_scaler_transform(Reqvalues[:, :], median_list, iqr_list)
                #**************************************#

                # Converting the time series to supervised frame
                features = np.shape(Reqvalues)[1]
                #lookback = model_properties['lookback']
                #horizon = model_properties['horizon']
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
                ###print (inv_yhat)

            data.loc[TempData.index.max(),str(self.config["metric"])] = inv_yhat
            if NumberDaysToPredict-ithDay == 0:
                break
            Lastdate = data.tail(NumberDaysToPredict-ithDay).index[0]
            modifiedDate = datetime.strptime(data.tail(NumberDaysToPredict-ithDay).index[0], "%Y-%m-%d")
            Firstdate = modifiedDate + timedelta(days=-(lookback+horizon-1))
            Firstdate = Firstdate.date().strftime('%Y-%m-%d')
        data = self.MakeHolidayForecastZero(data)


        # for i in range(1,13):
        #     daystartAhead=datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=i)
        #     dayendAhead=datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=i+1)
        #     self.nthDayPrediction(store_num, product_num,data, daystartAhead, dayendAhead,datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d"),Pacific,todayWithoutTZ)


        tendayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=int(self.config["shorter_forecast_start"]))
        fourteendayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=int(self.config["shorter_forecast_end"]))
        thirtydayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=int(self.config["longer_forecast_start"]))
        thirtyfourdayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=int(self.config["longer_forecast_end"]))
        redundancyAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=int(self.config["redundancy_forecast_start"]))
        redundancyfourdayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=int(self.config["redundancy_forecast_end"]))
        
        
        
        self.nthDayPrediction(store_num, product_num,data, tendayAhead, fourteendayAhead,datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d"),Pacific,todayWithoutTZ)
 
        self.nthDayPrediction(store_num, product_num,data, thirtydayAhead,thirtyfourdayAhead,datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d"),Pacific,todayWithoutTZ)
    
        self.nthDayPrediction(store_num, product_num,data, redundancyAhead,redundancyfourdayAhead,datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d"),Pacific,todayWithoutTZ)

        #self.s3toAurora(store_num)

        del multi_model
        del data
        del dataframe
        gc.collect()
        try:
            K.clear_session()
            tf.reset_default_graph()
        except:
            print ('tf graph session cleared')
        print (str(store_num),str(product_num)," is done")
        #**************************************End**************************************#

    def predict(self,store_num,product_num,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path,data_df):
        with graph.as_default():
            index = 0
            #sales_inference(self,index,store_num,product_num, bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path,data_df)
            return self.sales_inference(index,store_num,product_num, bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path,data_df)
            
             


import os

os.environ['KMP_DUPLICATE_LIB_OK']='True'


Infer=Inference()

if __name__=="__main__":
    #store_num = sys.argv[1]
    #product_num = sys.argv[2]
    bucket_name = ''#sys.argv[3]
    athena_csv_filepath = ''#sys.argv[4]
    mp_path = ''#sys.argv[5]
    h5_path = ''#sys.argv[6]
    upload_path = ''#sys.argv[7]
#   stores=[1895]
#     for store_num in stores:
#         s3_client = boto3.client('s3', region_name='us-east-1')
#         n=len(str(store_num))
#         nz=5-n
#         #key=s3_client.list_objects_v2(Bucket=bucket_name,Prefix='data_lake/Inference/ItemCount/location_num='+(str(0)*nz)+str(store_num))['Contents'][0]['Key']
#         key=s3_client.list_objects_v2(Bucket='prod-q-forecasting-artifacts',Prefix='data_lake/Inference/ItemCount/location_num='+str('0')*nz+str(store_num))['Contents'][0]['Key']
#         #df = pd.read_csv('Inference_daily.csv')
#         obj = s3_client.get_object(Bucket='prod-q-forecasting-artifacts', Key=key)
#         body = obj['Body']
#         csv_string = body.read().decode('utf-8')
#         df = pd.read_csv(StringIO(csv_string))
#         ########df = pd.read_csv('Inference_daily_868.csv')
#         productdata = df.groupby('product', sort=False)

#         for name, group in productdata:
#             if len(group) < 57:
#                 pass
#             else:
#                 Infer=Inference()
#                 store_num = group.location_num.unique()[0]
#                 product_num = group['product'].unique()[0]
#                 if product_num==160001:
#                     Infer.predict(store_num,product_num,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path,group)
# #                 else:
# #                     print ('skipping',product_num)
# #                 Infer.predict(store_num,product_num,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path,group)
# #                 break
#         Infer.combine_csv("./tmp/","forecast/itemcount/daily/",store_num)
#     #sales_inference(store_num,product_num,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path)
#     # Infer.predict(store_num,product_num,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path,group)






