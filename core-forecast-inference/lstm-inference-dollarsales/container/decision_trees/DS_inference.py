# Packages Needed


# Machine Learning Packages
import os
os.environ['TF_CPP_MIN_LOG_LEVEL']='3'
import pandas as pd
import numpy as np
np.random.seed(seed=786)
from sklearn.preprocessing import MinMaxScaler, RobustScaler
#from sklearn.externals import joblib
from numpy import concatenate
from sklearn.preprocessing import LabelEncoder
#from keras.models import load_model

# File Handling
import boto3
from boto3.s3.transfer import S3Transfer
from io import StringIO

import json
#import pickle
import sys
# Calendar / Time computation packages
import time
from datetime import datetime, date, timedelta
# Garbage Collector
import gc
secret = None


pd.options.mode.chained_assignment=None
from keras.models import load_model
from keras import backend as K

import tensorflow as tf
#tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
from pytz import timezone

import glob
import shutil
# Logging module to create a log file
import logging
# tqdm for smart progress bar
from tqdm import tqdm

global graph
from config import Config
#graph = tf.get_default_graph()
os.environ['TF_CPP_MIN_LOG_LEVEL']='3'
pd.options.mode.chained_assignment=None

formatter = logging.Formatter('%(asctime)s : %(levelname)s -- %(message)s')

def setup_logger(name, log_file, level=logging.INFO):
    """Function setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger
current_log_time = datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
mp_logging=setup_logger('training_inference','debug_log_files/Stack_inference_results'+'_'+current_log_time+'.log',level=logging.DEBUG)
# In[2]:
CONFIG = Config('lstm_automation_config_ds.json')
PARAMS = CONFIG.get_params()
class Inference():
    def __init__(self, cuda):
#        self.store_start = store_start
#        self.store_end = store_end
        
        self.run_mode = PARAMS["run_mode"]
        if self.run_mode == "local":
            os.environ['CUDA_VISIBLE_DEVICES'] = cuda
        self.bucket = PARAMS["bucket"]
        self.training_bucket = PARAMS["training_bucket"]
        self.base_key = PARAMS["base_key"]
        self.base_forecast_key = PARAMS["base_forecast_key"]
        self.random_seed = PARAMS["random_seed"]
        self.metric = PARAMS["metric"]
        self.robust_max = PARAMS["robust_max"]
        self.robust_min = PARAMS["robust_min"]
        self.training_cutoff = PARAMS["training_cutoff"]
        self.training_start = PARAMS["training_start"]
        self.inference_end_year = PARAMS["inference_end_year"]
        self.inference_end_month = PARAMS["inference_end_month"]
        self.inference_end_day = PARAMS["inference_end_day"]
        self.inference_lookback_index = PARAMS["inference_lookback_index"]
        self.forecast_output_window = PARAMS["forecast_output_window"]
        self.lookback = PARAMS["lookback"]
        self.horizon = PARAMS["horizon"]
        self.neuron = PARAMS["neuron"]
        self.loss_metric = PARAMS["loss_metric"]
        self.optimizer = PARAMS["optimizer"]
        self.epochs = PARAMS["epochs"]
        self.batch_size = PARAMS["batch_size"]
        self.verbose = PARAMS["verbose"]
        self.do_inference = PARAMS["do_inference"]
        self.federal_holidays = PARAMS["federal_holidays"]
        self.columns_ic = PARAMS["columns_ic"]
        self.columns_ds = PARAMS["columns_ds"]
        self.features_to_use_ic = PARAMS["features_to_use_ic"]
        self.features_to_use_ds = PARAMS["features_to_use_ds"]
        self.features_to_use_tc = PARAMS["features_to_use_tc"]
        self.upload_completed_model = PARAMS["upload_completed_model"]
        self.experiment_purpose = PARAMS["experiment_purpose"]
        self.experiment_month = PARAMS["experiment_month"]
        self.model_version = PARAMS["model_version"]
        self.custom_transform_type = PARAMS["custom_transform_type"]
        self.lam = 0
        self.avg_ic = 0
        self.min_ic = 0
        self.max_ic = 0
        self.inference_history_lookback = PARAMS["inference_history_lookback"]
        self.shorter_forecast_start = PARAMS["shorter_forecast_start"]
        self.shorter_forecast_end = PARAMS["shorter_forecast_end"]
        self.longer_forecast_start = PARAMS["longer_forecast_start"]
        self.longer_forecast_end = PARAMS["longer_forecast_end"]
        self.redundancy_forecast_start = PARAMS["redundancy_forecast_start"]
        self.redundancy_forecast_end = PARAMS["redundancy_forecast_end"]
        self.forecast_output_window = PARAMS["forecast_output_window"]
        self.use_diff_scaler = PARAMS["use_diff_scaler"]
        self.saturday_closed_stores = PARAMS["saturday_closed_stores"]
        self.train_remod = PARAMS["train_remod"]
        self.crawler_key = PARAMS["s3_crawler_params_key"]
        self.version_table_path = PARAMS["version_table"]
        self.s3_final_store_list_path = PARAMS["s3_final_store_list"]
        self.s3_final_store_list_extended_path = PARAMS["s3_final_store_list_extended"]
        self.forecast_period = PARAMS["forecast_period"]
        self.shorter_combined_fname = PARAMS["shorter_combined_fname_ic"]
        self.longer_combined_fname = PARAMS["longer_combined_fname_ic"]
        self.redundancy_combined_fname = PARAMS["redundancy_combined_fname_ic"]
        if self.metric == "sales_sub_total":
            self.metric_key = "ds"
        elif self.metric == "trans_count":
            self.metric_key = "tc"
        elif self.metric == "sum_daily_quantity":
            self.metric_key = "ic"
        self.config = CONFIG
        self.params = PARAMS

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


    def new_outlier_impute_dayofweek(self,df, colname,lookback_outlier):
        '''
        Dayofweek-wise outlier imputation
        '''
        outlier_record = {"location_num": [], "product": [], "business_date": [], "dayofweek": [], "median": [], "lower_limit": [], "upper_limit": [], "actuals": [], "imputed_actuals": [], "generation_date": []}
        df = df.sort_index()
        df_rel = df.head(lookback_outlier).copy()
        non_zero = df_rel[(df_rel['dayofweek']!=7) & (df_rel['aroundchristmas']==0) &\
                         (df_rel['onedaypriorchristmas_and_new_year']==0) &\
                         (df_rel['blackfridaycheck']==0) & \
                         (df_rel['holiday']==0) & \
                         (df_rel['aroundthanksgiving']==0) & \
                         (df_rel['federalholiday']==0)  & \
                         ~((df_rel['dayofweek']==6)&(df_rel['location_num'].isin(self.saturday_closed_stores)))]



        if(not non_zero[non_zero[colname]>0.001].empty):
#        if True:
            q1_col = non_zero.groupby(by='dayofweek')[colname].quantile(0.25)
            q3_col = non_zero.groupby(by='dayofweek')[colname].quantile(0.75)

            lower_limit_col = q1_col - 1.5*(q3_col - q1_col)
            upper_limit_col = q3_col + 1.5*(q3_col - q1_col)

            df = df.assign(lower_limit = df['dayofweek'].map(lower_limit_col))
            df = df.assign(upper_limit = df['dayofweek'].map(upper_limit_col))

            df['outliers'] = ((df[colname]<df['lower_limit'])|(df[colname]>df['upper_limit']))&(df.index.isin(non_zero.index))
            non_zero = non_zero.assign(lower_limit = non_zero['dayofweek'].map(lower_limit_col))
            non_zero = non_zero.assign(upper_limit = non_zero['dayofweek'].map(upper_limit_col))

            non_zero['outliers']=((non_zero[colname]<non_zero['lower_limit'])|(non_zero[colname]>non_zero['upper_limit']))

            num_outliers=len(df[df['outliers']==True])

            dayofweek_median = non_zero[non_zero['outliers']==False].groupby(by='dayofweek')[colname].median()

            if(not non_zero[non_zero.outliers==True].empty):

                dayofweek_median = non_zero[non_zero['outliers']==False].groupby(by='dayofweek')[colname].median()


                df = df.assign(dayofweek_median_col=df['dayofweek'].map(dayofweek_median))
                if self.metric_key == "ic":
                    records_df = df.loc[df['outliers']==True, ['location_num', 'product','dayofweek', 'dayofweek_median_col', colname, 'dayofweek_median_col', 'lower_limit', 'upper_limit']]
                else:
                    records_df = df.loc[df['outliers']==True, ['location_num', 'dayofweek', 'dayofweek_median_col', colname, 'dayofweek_median_col', 'lower_limit', 'upper_limit']]
#                records_df.loc[:,"business_date"] = records_df.index
                records_df.reset_index(inplace=True)
#                print(records_df)
                outlier_record["location_num"]=list(records_df.loc[:,"location_num"].values)
                if self.metric_key == "ic":
                    outlier_record["product"]=list(records_df.loc[:,"product"].values)
                else:
                    outlier_record["product"]=[999999]*len(records_df.loc[:,"location_num"].values)
                outlier_record["business_date"]=list(records_df.loc[:,"business_date"].values)
                outlier_record["dayofweek"]=list(records_df.loc[:,"dayofweek"].values)
                outlier_record["median"]=list(records_df.loc[:,"dayofweek_median_col"].values[:,0])
                outlier_record["lower_limit"]=list(records_df.loc[:,"lower_limit"].values)
                outlier_record["upper_limit"]=list(records_df.loc[:,"upper_limit"].values)
                outlier_record["actuals"]=list(records_df.loc[:,colname].values)
                outlier_record["imputed_actuals"]=list(records_df.loc[:,"dayofweek_median_col"].values[:,0])
                outlier_record["generation_date"]=[datetime.now().strftime("%Y-%m-%d")]*len(records_df.loc[:,"location_num"].values)

                outlier_df=pd.DataFrame(data=outlier_record, columns=["location_num", "product", "business_date", "dayofweek", "median", "lower_limit", "upper_limit", "actuals", "imputed_actuals", "generation_date"])
                if self.metric_key == "ic":
                    pred_fname = self.config.get_location("local_outlier_impute_file_"+self.metric_key, [str(outlier_record["location_num"][0]).zfill(5), str(outlier_record["product"][0])])
                else:
                    pred_fname = self.config.get_location("local_outlier_impute_file_"+self.metric_key, [str(outlier_record["location_num"][0]).zfill(5)])
                if self.run_mode == "prod":
                    outlier_df.to_csv(pred_fname, index=False)
                    s3 = boto3.client('s3')
                    transfer = S3Transfer(s3)
                    
                    transfer.upload_file(pred_fname, self.bucket, self.config.get_location("s3_outlier_imputation_key_"+self.metric_key, [str(outlier_record["location_num"][0]).zfill(5)])+pred_fname)
                    
                    
                    os.remove(pred_fname)
                
                
                df.loc[df.outliers==True,colname] = df[df.outliers==True]['dayofweek_median_col']
                df[colname].fillna(non_zero[non_zero['outliers']==False][colname].median(),inplace=True)
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

    
    def nthDayPrediction(self, location_num, product_num,PredictedData, nthStartDayDate, nthEndDayDate,firstDayDate,Pacific,todayWithoutTZ):
        
        nthStartDayDateStr = nthStartDayDate.date().strftime('%Y-%m-%d')
        nthEndDayDateStr = nthEndDayDate.date().strftime('%Y-%m-%d')
        # Loading the master daily data from S3 into memory

        nthDaySales = PredictedData.loc[nthStartDayDateStr: nthEndDayDateStr, self.metric]

        # Saving the predictions
        if self.metric_key == "ic":
            df = pd.DataFrame(columns=['location_num', 'product','businessdate', 'forecast','generation_date'], \
              data= {'location_num':str(location_num),
                     'product': str(product_num),
                     'businessdate':nthDaySales.index,
                     'forecast': nthDaySales.values,
                     'generation_date':datetime.strftime(datetime.now(tz=Pacific),'%Y-%m-%d') })
        else:
            df = pd.DataFrame(columns=['location_num', 'businessdate', 'forecast','generation_date'], \
              data= {'location_num':str(location_num),
                     'businessdate':nthDaySales.index,
                     'forecast': nthDaySales.values,
                     'generation_date':datetime.strftime(datetime.now(tz=Pacific),'%Y-%m-%d') })

        df['forecast'] = df['forecast'].apply(lambda x: 0 if x<0 else x)

        nDays = nthStartDayDate - firstDayDate
        #bucket=str(self.config["bucket"])
        
        ## Make the if conditions a function of the values in the config file
        if nDays.days >= int(self.longer_forecast_start):
            if nDays.days <= int(self.longer_forecast_end):
                pathToSave = self.base_forecast_key + self.config.get_location("s3_inference_key_longer_"+self.metric_key,[str(location_num).zfill(5)])
                if self.metric_key == "ic":
                    pred_fname = self.config.get_location("local_inference_longer_"+self.metric_key,[str(location_num).zfill(5), str(product_num)])
                else:
                    pred_fname = self.config.get_location("local_inference_longer_"+self.metric_key,[str(location_num).zfill(5)])
                
                target_dir = '/tmp/'
                df.to_csv(target_dir+pred_fname, index=False)
                s3 = boto3.client('s3')
                transfer = S3Transfer(s3)
                if self.metric_key != "ic":
                    transfer.upload_file(os.path.join(target_dir, pred_fname), self.bucket, pathToSave+"/"+pred_fname)
                    os.remove(os.path.join(target_dir, pred_fname))
            else:
                pathToSave = self.base_forecast_key + self.config.get_location("s3_inference_key_redundancy_"+self.metric_key,[str(location_num).zfill(5)])
                if self.metric_key == "ic":
                    pred_fname = self.config.get_location("local_inference_redundancy_"+self.metric_key,[str(location_num).zfill(5), str(product_num)])
                else:
                    pred_fname = self.config.get_location("local_inference_redundancy_"+self.metric_key,[str(location_num).zfill(5)])
                
                target_dir = '/tmp/'
                df.to_csv(target_dir+pred_fname, index=False)
                s3 = boto3.client('s3')
                transfer = S3Transfer(s3)
                if self.metric_key != "ic":
                    transfer.upload_file(os.path.join(target_dir, pred_fname), self.bucket, pathToSave+"/"+pred_fname)
                    os.remove(os.path.join(target_dir, pred_fname))

        else:
            pathToSave = self.base_forecast_key + self.config.get_location("s3_inference_key_shorter_"+self.metric_key,[str(location_num).zfill(5)])
            if self.metric_key == "ic":
                pred_fname = self.config.get_location("local_inference_shorter_"+self.metric_key,[str(location_num).zfill(5), str(product_num)])
            else:
                pred_fname = self.config.get_location("local_inference_shorter_"+self.metric_key,[str(location_num).zfill(5)])
            
            target_dir = '/tmp/'
            df.to_csv(target_dir+pred_fname, index=False)
            s3 = boto3.client('s3')
            transfer = S3Transfer(s3)
            if self.metric_key != "ic":
                transfer.upload_file(os.path.join(target_dir, pred_fname), self.bucket, pathToSave+"/"+pred_fname)
                os.remove(os.path.join(target_dir, pred_fname))
    

    

    def combine_csv(self,location_num):

        s3 = boto3.client('s3')
        transfer = S3Transfer(s3)
        target_dir = '/tmp/'
        
        
        paths = {"shorter": {
            "pred_fname_shorter": self.config.get_location("local_inference_shorter_"+self.metric_key,["*", "*"]),
            "combined_pred_fname_shorter": self.shorter_combined_fname,
            "s3_upload_shorter": self.base_forecast_key + self.config.get_location("s3_inference_key_shorter_"+self.metric_key,[str(location_num).zfill(5)]) + "/" + self.shorter_combined_fname
        },
          "longer": {
            "pred_fname_longer": self.config.get_location("local_inference_longer_"+self.metric_key,["*", "*"]),
            "combined_pred_fname_longer": self.longer_combined_fname,
            "s3_upload_longer": self.base_forecast_key + self.config.get_location("s3_inference_key_longer_"+self.metric_key,[str(location_num).zfill(5)]) + "/" + self.longer_combined_fname
        },
            "redundancy": {
            "pred_fname_redundancy": self.config.get_location("local_inference_redundancy_"+self.metric_key,["*", "*"]),
            "combined_pred_fname_redundancy": self.redundancy_combined_fname,
            "s3_upload_redundancy": self.base_forecast_key + self.config.get_location("s3_inference_key_redundancy_"+self.metric_key,[str(location_num).zfill(5)]) + "/" + self.redundancy_combined_fname
        }}
        
        
        #shorter_pred_fname = self.config.get_location("local_inference_shorter_"+self.metric_key,["*", "*"])
        #longer_pred_fname = self.config.get_location("local_inference_longer_"+self.metric_key,["*", "*"])
        #redundancy_pred_fname = self.config.get_location("local_inference_redundancy_"+self.metric_key,["*", "*"])
        #shorter_combined_fname = self.shorter_combined_fname
        #longer_combined_fname = self.longer_combined_fname
        #redundancy_combined_fname = self.redundancy_combined_fname
        
        for key in paths.keys():
            
            allFiles = glob.glob(target_dir+ paths[key]["pred_fname_"+key])
            with open(target_dir + paths[key]["combined_pred_fname_"+key], 'wb') as outfile:
                for i, fname in enumerate(allFiles):
                    with open(fname, 'rb') as infile:
                        if i != 0:
                            infile.readline()  # Throw away header on all but first file
                            # Block copy rest of file from input to output without parsing
                        shutil.copyfileobj(infile, outfile)
                        #print(fname + " has been imported.")
            df_10 = pd.read_csv(target_dir + paths[key]["combined_pred_fname_"+key])
            df_10.to_csv(target_dir + paths[key]["combined_pred_fname_"+key],index=False,header=False)
            #pathToSave = str(path_save)+'14thday/location_num='+str(store_num)+'/'
            #pred_fname = 'fourteendayAhead.csv'
            transfer.upload_file(os.path.join(target_dir, paths[key]["combined_pred_fname_"+key]), self.bucket,
                                 paths[key]["s3_upload_"+key])
            time.sleep(0.02)
            #shutil.rmtree(os.path.join(str(infiles_path),"ten"))
            os.remove(os.path.join(target_dir, paths[key]["combined_pred_fname_"+key]))
            #os.remove(os.path.join(target_dir, paths[key]["pred_fname_"+key]))



    def ZeroImputing(self,data, metric):
        '''
        Impute zero-actuals (non-holidays) with dayofweek-wise median
        '''
        df=data
        df_rel=df.head(self.inference_history_lookback)
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
        '''
        Forecast 0s
        '''
        tendayAhead = datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=self.shorter_forecast_start-1)
        fourteendayAhead = datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=self.shorter_forecast_end-1)
        thirtydayAhead = datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=self.longer_forecast_start-1)
        thirtyfourdayAhead = datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=self.longer_forecast_end-1)
        redundancyAhead = datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=self.redundancy_forecast_start-1)
        redundancyfourdayAhead = datetime.strptime(todayWithoutTZ, "%Y-%m-%d")+ timedelta(days=self.redundancy_forecast_end-1)

        data = pd.DataFrame(columns=[self.metric],data={self.metric:np.zeros((self.forecast_output_window+1,))})

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
        '''
        Hardcode holiday forecast = 0
        '''
        ForecastedData.loc[ForecastedData['holiday'] == 1,self.metric] = 0
        return ForecastedData



    def sales_inference_no_s3(self,location_num,product_num, bucket_name,athena_csv_filepath,model_properties,multi_model,upload_path,data):

        '''
        Main function -- pull model artifacts, data and generate rolling forecast
        '''
        
        
        NumberDaysToPredict = self.forecast_output_window
        if self.run_mode == "prod":
            
            
            s3 = boto3.client('s3')
            if self.metric_key == "ic":
                file_name = self.config.get_location("inference_data_key_"+self.metric_key, [str(location_num).zfill(5)])
                folder_name = self.config.get_location(
                "s3_model_object_folder_name_"+self.metric_key, [
                    str(location_num), str(product_num)])
                json_fname = self.config.get_location("s3_model_props_json_fname_"+self.metric_key, \
                [str(location_num), str(product_num)])
                model_name = self.config.get_location(
                "s3_model_object_model_name_"+self.metric_key, [
                    str(location_num), str(product_num)])
            else:
                file_name = self.config.get_location("inference_data_key_ds", [str(location_num).zfill(5)])
                folder_name = self.config.get_location(
                "s3_model_object_folder_name_"+self.metric_key, [
                    str(location_num)])
                json_fname = self.config.get_location("s3_model_props_json_fname_"+self.metric_key, \
                [str(location_num)])
                model_name = self.config.get_location(
                "s3_model_object_model_name_"+self.metric_key, [
                    str(location_num)])
 
            # Loading the inference data
            obj = s3.get_object(Bucket=self.bucket, Key=file_name)
            body = obj['Body']
            csv_string = body.read().decode('utf-8')
            data = pd.read_csv(StringIO(csv_string))
            
            # Loading the model properties
            
            prop_fname = self.base_key + folder_name + "/" + json_fname
            
            obj = s3.get_object(Bucket= self.bucket, Key= prop_fname)
            model_properties = json.loads(obj['Body'].read().decode('utf-8'))
            current_dt = model_properties['execution_date']
            #**************************************#

            # Loading the model.h5 file
            
            model_fname = self.base_key + folder_name + "/" + model_name
            result = s3.download_file(self.bucket, model_fname, '/tmp/'+model_name)
            multi_model = load_model('/tmp/'+model_name)
            os.remove('/tmp/'+model_name)
            
            
            
       
        # Set indexes to business_date and keep a backup column with dates in it
        if self.run_mode == "prod":
            data.sort_values('business_date', inplace=True)
            data['business_date_backUp'] = data['business_date']
            data = data.set_index('business_date')
        if self.metric_key == "ic":
            data = data[data["product"] == int(product_num)]
        # Temp date is the day we need to start forecasting for, Hence take lower 32 days.
        Tempdate = data.tail(NumberDaysToPredict).index[0]
        
        Tempdate=str(Tempdate)
        Tempdate=Tempdate.split(" ")[0]
        
        # Convert to datetime format to calculate any missing dates if at all.
        modifiedDate = datetime.strptime(Tempdate, "%Y-%m-%d")


        #Define timeZone, Why Pacific? We will have time till 3AM ET to run our pipeline still having same date.
        Pacific = timezone('US/Pacific')
        today = datetime.now(tz=Pacific)

        todayWithoutTZ = datetime.strftime(today,'%Y-%m-%d')

        dataframe = data
        store_num = data.location_num.unique()[0]
        
        start = time.time()

        data = self.make_it_zero(data)
        data = self.ZeroImputing(data,metric=self.metric)

        if self.run_mode == "prod":
           data = pd.get_dummies(data, columns=['federalholiday_name'])

        
        current_dt = model_properties['execution_date']
        

        # Scaler Implementation
        median_list = model_properties['median_list']
        lookback = model_properties['lookback']
        horizon = model_properties['horizon']
        median_list = [float(x) for x in median_list]
        iqr_list = model_properties['iqr_list']
        iqr_list = [float(x) for x in iqr_list]
        np.random.seed(seed=self.random_seed)
        
        data['location_num']=store_num
        colname = self.metric
        data = self.new_outlier_impute_dayofweek(data, colname,lookback_outlier=self.inference_history_lookback)
        print(data.head())
        ##################### REMODELING - REPLACE WITH HISTORY ###############################
        middle_days = len(data.index) - (self.inference_history_lookback + NumberDaysToPredict)
        if middle_days>0:
            
            if self.run_mode == "prod":
                target_weekday = data[data.index==datetime.strftime(datetime.strptime(data.index.max(), "%Y-%m-%d")+timedelta(days=-NumberDaysToPredict), "%Y-%m-%d")]["dayofweek"].values[0]
                

                history_data = data.head(self.inference_history_lookback)
                history_end_date = history_data[history_data["dayofweek"]==target_weekday].index.max()
                print(history_end_date)
                if middle_days>=(self.lookback+self.horizon):
                    history_start_date = datetime.strftime(datetime.strptime(history_end_date, "%Y-%m-%d")+timedelta(days=-(self.lookback+self.horizon-1)), "%Y-%m-%d")
                else:
                    history_start_date = datetime.strftime(datetime.strptime(history_end_date, "%Y-%m-%d")+timedelta(days=-middle_days+1), "%Y-%m-%d")
                  
                req_data = history_data.loc[history_start_date:history_end_date, self.metric]
                
                inf_end_date = datetime.strftime(datetime.strptime(data.index.max(), "%Y-%m-%d")+timedelta(days=-NumberDaysToPredict), "%Y-%m-%d")
                if middle_days>=(self.lookback+self.horizon):
                    inf_start_date = datetime.strftime(datetime.strptime(inf_end_date, "%Y-%m-%d")+timedelta(days=-(self.lookback+self.horizon-1)), "%Y-%m-%d")
                else:
                    inf_start_date = datetime.strftime(datetime.strptime(inf_end_date, "%Y-%m-%d")+timedelta(days=-middle_days+1), "%Y-%m-%d")
                
                data.loc[inf_start_date:inf_end_date,self.metric] = req_data.values
            else:
                target_weekday = data[data.index==datetime.strftime(data.index.max()+timedelta(days=-NumberDaysToPredict), "%Y-%m-%d")]["dayofweek"].values[0]
                

                history_data = data.head(self.inference_history_lookback)
                history_end_date = history_data[history_data["dayofweek"]==target_weekday].index.max()
                if middle_days>=(self.lookback+self.horizon):
                    history_start_date = datetime.strftime(history_end_date+timedelta(days=-(self.lookback+self.horizon-1)), "%Y-%m-%d")
                else:
                    history_start_date = datetime.strftime(history_end_date+timedelta(days=-middle_days+1), "%Y-%m-%d")
                
                req_data = history_data.loc[history_start_date:history_end_date, self.metric]
               
                inf_end_date = datetime.strftime(data.index.max()+timedelta(days=-NumberDaysToPredict), "%Y-%m-%d")
                if middle_days>=(self.lookback+self.horizon):
                    inf_start_date = datetime.strftime(datetime.strptime(inf_end_date, "%Y-%m-%d")+timedelta(days=-(self.lookback+self.horizon-1)), "%Y-%m-%d")
                else:
                    inf_start_date = datetime.strftime(datetime.strptime(inf_end_date, "%Y-%m-%d")+timedelta(days=-middle_days+1), "%Y-%m-%d")
                data.loc[inf_start_date:inf_end_date,self.metric] = req_data.values
        ######################################################################################

        Lastdate = modifiedDate + timedelta(days=0)
        
        Firstdate = modifiedDate + timedelta(days=-(self.lookback + self.horizon - 1)) 
        
        Firstdate = Firstdate.date().strftime('%Y-%m-%d')
        Lastdate = Lastdate.date().strftime('%Y-%m-%d')
        
        for ithDay in range(1,NumberDaysToPredict+1):
            
            TempData = data.loc[Firstdate:Lastdate].reindex(columns=model_properties['features_used_in_model'])
            
            # temporary fix to fill NaN federal holiday features with 0, to be removed once schema is changed
            for col in list(TempData.columns.values):
                if col.startswith('federalholiday_name'):
                    TempData[col].fillna(0, inplace=True)

            
            if TempData.loc[TempData.index.max(),'dayofweek'] == 7 or (TempData.loc[TempData.index.max(),'dayofweek'] == 6 and str(store_num) in self.saturday_closed_stores):
                inv_yhat = 0
            else:

                feat_df = TempData[model_properties['features_used_in_model']]
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


                features_drop=[]



                try:
                    for feat in model_properties['features_to_drop']:

                        features_drop =features_drop + [x*features + model_properties['features_used_in_model'].index(feat) \
                        for x in range(0, lookback+ horizon)]
                except:
                    pass

                reframed.drop(reframed.columns[Temp+features_drop], axis=1, inplace=True)



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

            data.loc[TempData.index.max(),self.metric] = inv_yhat


            if NumberDaysToPredict-ithDay == 0:
                break
            Lastdate = data.tail(NumberDaysToPredict-ithDay).index[0]
            modifiedDate = datetime.strptime(str(data.tail(NumberDaysToPredict-ithDay).index[0]).split(" ")[0], "%Y-%m-%d")
            Firstdate = modifiedDate + timedelta(days=-(self.lookback + self.horizon - 1))
            Firstdate = Firstdate.date().strftime('%Y-%m-%d')

        tendayAhead = datetime.strptime(str(data.tail(NumberDaysToPredict).index[0]).split(" ")[0], "%Y-%m-%d")+ timedelta(days=self.shorter_forecast_start)
        fourteendayAhead = datetime.strptime(str(data.tail(NumberDaysToPredict).index[0]).split(" ")[0], "%Y-%m-%d")+ timedelta(days=self.shorter_forecast_end)
        thirtydayAhead = datetime.strptime(str(data.tail(NumberDaysToPredict).index[0]).split(" ")[0], "%Y-%m-%d")+ timedelta(days=self.longer_forecast_start)
        thirtyfourdayAhead = datetime.strptime(str(data.tail(NumberDaysToPredict).index[0]).split(" ")[0], "%Y-%m-%d")+ timedelta(days=self.longer_forecast_end)


        redundancyAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=self.redundancy_forecast_start)
        redundancyfourdayAhead = datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d")+ timedelta(days=self.redundancy_forecast_end)

        data = self.MakeHolidayForecastZero(data)

        if 2 in self.forecast_period:
            self.nthDayPrediction(location_num, product_num,data, tendayAhead, fourteendayAhead,datetime.strptime(str(data.tail(NumberDaysToPredict).index[0]).split(" ")[0], "%Y-%m-%d"),Pacific,todayWithoutTZ)

        if 3 in self.forecast_period:
            self.nthDayPrediction(location_num, product_num,data, thirtydayAhead,thirtyfourdayAhead,datetime.strptime(str(data.tail(NumberDaysToPredict).index[0]).split(" ")[0], "%Y-%m-%d"),Pacific,todayWithoutTZ)

        if 4 in self.forecast_period:
            self.nthDayPrediction(location_num, product_num,data, redundancyAhead,redundancyfourdayAhead,datetime.strptime(data.tail(NumberDaysToPredict).index[0], "%Y-%m-%d"),Pacific,todayWithoutTZ)


        if self.run_mode == "prod":
            del multi_model
        del data
        del dataframe
        gc.collect()
#         if self.run_mode == "prod":
#             try:
#                K.clear_session()
#                tf.reset_default_graph()
#             except:
#                print ('tf graph session cleared')
        #print (str(store_num),str(product_num)," is done")
        #**************************************End**************************************#

    def predict(self,store_num,product_num,bucket_name,athena_csv_filepath,mp,h5,upload_path,data_df, quarter):
        '''
        Liason for "external" function calls
        '''
        self.quarter = quarter
        os.environ['TF_CPP_MIN_LOG_LEVEL']='3'
        graph=tf.get_default_graph()
        with graph.as_default():
            
            return self.sales_inference_no_s3(store_num,product_num, bucket_name,athena_csv_filepath,mp,h5,upload_path,data_df)

#infer = Inference(0)

#infer.predict('1895','160001','','','','','','','')
#if infer.metric_key == "ic":
#    infer.combine_csv('1895')

