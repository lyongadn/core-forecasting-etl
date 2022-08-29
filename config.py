import boto3
import json

class Config:
    config = {}
    
    def __init__(self,config):
        self.config = config
        
    @classmethod
    def from_event(cls,temp):
        return cls(config= temp)
        
    @classmethod
    def from_s3(cls,bucket,key):
        client = boto3.client('s3')
        return cls(config =  json.loads(client.get_object(Bucket=bucket,Key=key)['Body'].read().decode('utf-8')))
        
    def get_config(self):
        return self.config

    def get_data_bucket(self):
        return self.config['Inference_Data_Bucket']
        
    def get_secret_key(self):
        return self.config['secret_key']
    
    def get_database(self):
        return self.config['database']

    def get_15min_key(self,store_num):
        return self.config['15min_key'].replace('--',store_num)

    def get_daily_key(self,store_num):
        return self.config['daily_key'].replace('--',store_num)
    
    def get_daily_key_staging(self,store_num):
        return self.config['daily_key_staging'].replace('--',store_num)

    def get_15min_key_staging(self,store_num):
        return self.config['15min_key_staging'].replace('--',store_num)

    def get_ingredient_key(self,store_num):
        return self.config['ingredient_key'].replace('--',store_num)
    
    def get_ingredient_key_stag(self,store_num):
        return self.config['ingredient_key_staging'].replace('--',store_num)

    def get_store_list(self):
        return self.config['store_list']

    def get_sleep_time(self):
        return self.config['sleep_time']

    def get_15min_columns(self):
        return self.config['15min_table_schema'].split(',')

    def get_15min_local_path(self):
        return self.config['15min_local_path']
    
    def get_15min_local_path_staging(self):
        return self.config['15min_local_path_staging']

    def get_daily_columns(self):
        return self.config['daily_table_schema'].split(',')

    def get_ingredient_columns(self):
        return self.config['ingredient_table_schema'].split(',')

    def get_daily_local_path(self):
        return self.config['daily_local_path']
    
    def get_daily_local_path_staging(self):
        return self.config['daily_local_path_staging']

    def get_ingredient_local_path(self):
        return self.config['ingredient_local_path']
    
    def get_ingredient_local_path_stag(self):
        return self.config['ingredient_local_path_staging']

    def get_replica_host_link(self):
        return self.config['replica_host_link']

    def get_custom_host_link(self):
        return self.config['custom_host_link']

    def get_host_link(self):
        return self.config['host_link']

    def get_invoke_function_name(self):
        return self.config['lambda_function_name']

    def get_user_name(self):
        return self.config['user_name']
    
    def get_rds_password(self):
        return self.config['rds_password']

    def get_prod_bucket(self):
        return self.config['prod_bucket']

    def get_15min_database(self):
        return self.config['15min_database']

    def get_qc_database(self):
        return self.config['qc_database']

    def get_daily_database(self):
        return self.config['daily_database']

    def get_ingredient_database(self):
        return self.config['ingredient_database']

    def get_15min_table(self):
        return self.config['15min_table_name']

    def get_qc_table(self):
        return self.config['qc_table_name']

    def get_daily_table(self):
        return self.config['daily_table_name']

    def get_ingredient_table(self):
        return self.config['ingredient_table_name']        

    def get_data_bucket_prefix(self):
        return self.config['Inference_CSV_Prefix']

    def get_create_lambda_name(self):
        return self.config['CreateInfraFunction']

    def get_crawler_bucket(self):
        return self.config['crawler_bucket']

    def get_crawler_key(self):
        return self.config['crawler_key']

    def get_delete_lambda_name(self):
        return self.config['DeleteInfraFunction']

    def get_run_lambda_name(self):
        return self.config['RunInfraFunction']

    def get_docker_uri(self):
        return self.config['docker_image']

    def get_iam_role(self):
        return self.config['IAM_Role']

    def get_model_name(self):
        return self.config['ModelName']

    def get_endpoint_config_name(self):
        return self.config['EndpointConfigName']

    def get_instance_type(self):
        return self.config['InstanceType']

    def get_endpoint_name(self):
        return self.config['EndpointName']

    def get_batch_inference(self):
        return self.config['batch']

    def get_artifact_bucket(self):
        return self.config['Artifact_Bucket']

    def get_mp_path(self):
        return self.config['mp-path']

    def get_h5_path(self):
        return self.config['h5-path']

    def get_upload_path(self):
        return self.config['uploadPath']

    def get_upload_path_test(self):
        return self.config['upload_path_test']

    def get_summaryjson_upload_path_test(self):
        return self.config['summaryjson_upload_path_test']
        
    def set_store_num(self,location_num):
        self.config['store_num'] = location_num
    
    def set_status(self,status):
        self.config['status'] = status
   
    def set_period(self,period):
        self.config['period'] = period
    
    def get_status(self):
        return self.config['status']
   
    def get_period(self):
        return self.config['period']
        
    def get_store_num(self):
        return self.config['store_num']
    
    def get_data_key(self,store_num):
        return self.config['data_key_string'].replace('--location_num--',store_num)

    def get_local_name(self):
        return self.config['local_name']
    
    def get_inference_dollars_input_columns(self):
        return self.config['inference_dollars_input_table_schema'].split(',')
    
    def get_inference_dollars_input_local_path(self):
        return self.config['inference_dollars_input_local_path']
   
    def get_numberdays_topredict(self):
        return self.config['numberdays_topredict']
        
    """"""""""""""""""""""""""" Forecast Breakdown DollarTranscount """""""""""""""""""""""""""
    def get_inference_dollars_input_key(self,store_num):
        return self.config['inference_dollars_input_key'].replace('--',store_num)
        
    def get_dollar_lstm10_localpath(self):
        return self.config['dollar_lstm10_localpath']

    def get_dollar_lstm30_localpath(self):
        return self.config['dollar_lstm30_localpath']
    
    def get_dollar_lstm10_localpath_stg(self):
        return self.config['dollar_lstm10_localpath_stg']

    def get_dollar_lstm30_localpath_stg(self):
        return self.config['dollar_lstm30_localpath_stg']

    def get_transcount_lstm10_localpath(self):
        return self.config['transcount_lstm10_localpath']

    def get_transcount_lstm30_localpath(self):
        return self.config['transcount_lstm30_localpath']

    def get_forecast_columns(self):
        return self.config['forecast_table_schema'].split(',')

    def get_dollar_lstm10_key(self,store_num):
        return self.config['dollar_lstm10_key'].replace('--',store_num)

    def get_dollar_lstm30_key(self,store_num):
        return self.config['dollar_lstm30_key'].replace('--',store_num)
    
    def get_dollar_lstm10_key_stg(self,store_num):
        return self.config['dollar_lstm10_key_stg'].replace('--',store_num)

    def get_dollar_lstm30_key_stg(self,store_num):
        return self.config['dollar_lstm30_key_stg'].replace('--',store_num)

    def get_transcount_lstm10_key(self,store_num):
        return self.config['transcount_lstm10_key'].replace('--',store_num)

    def get_transcount_lstm30_key(self,store_num):
        return self.config['transcount_lstm30_key'].replace('--',store_num)

    def get_dollar_10_table(self):
        return self.config['dollar_10days_table_name']

    def get_dollar_30_table(self):
        return self.config['dollar_30days_table_name']

    def get_transcount_10_table(self):
        return self.config['transcount_10days_table_name']

    def get_transcount_30_table(self):
        return self.config['transcount_30days_table_name']

    def get_dollar_10_upload_table(self):
        return self.config['dollar_10days_upload_table_name']

    def get_dollar_30_upload_table(self):
        return self.config['dollar_30days_upload_table_name']

    def get_transcount_10_upload_table(self):
        return self.config['transcount_10days_upload_table_name']

    def get_transcount_30_upload_table(self):
        return self.config['transcount_30days_upload_table_name']
        
    def get_dollar_ratio_table(self):
        return self.config['dollar_ratio_table_name']

    def get_transcount_ratio_table(self):
        return self.config['transcount_ratio_table_name']
    
    """"""""""""""""""""""""""" Inference Item Count Input """""""""""""""""""""""""""
    def get_feature_days(self):
        return self.config['numberdays_topredict']
    
    def get_actual_days(self):
        return self.config['actual_days']
    
    def get_features(self):
        return self.config['features']
    
    def get_inference_iteminputcolumns(self):
        return self.config['inference_iteminput_table_schema'].split(',')
    
    def get_inferenceinput_itemlocalpath(self):
        return self.config['inferenceinput_itemlocalpath']
    
    def get_inferenceinput_itemkey(self,store_num):
        return self.config['inferenceinput_itemkey'].replace('--',store_num)
        
    """"""""""""""""""""""""""" Forecast Breakdown Itemcount """""""""""""""""""""""""""
    def get_lstm10_local_path(self):
        return self.config['lstm10_localpath']
    
    def get_lstm30_local_path(self):
        return self.config['lstm30_localpath']
    
    def get_lstm10_local_path_stg(self):
        return self.config['lstm10_localpath_stg']

    def get_lstm30_local_path_stg(self):
        return self.config['lstm30_localpath_stg']
    
    def get_lstm10_upload_key(self,store_num):
        return self.config['lstm10_key'].replace('--',store_num)
    
    def get_lstm30_upload_key(self,store_num):
        return self.config['lstm30_key'].replace('--',store_num)
    
    def get_lstm10_upload_key_stg(self,store_num):
        return self.config['lstm10_key_stg'].replace('--',store_num)

    def get_lstm30_upload_key_stg(self,store_num):
        return self.config['lstm30_key_stg'].replace('--',store_num)
    
    def get_lstm10_upload_table(self):
        return self.config['lstm10_upload_table_name']
    
    def get_lstm30_upload_table(self):
        return self.config['lstm30_upload_table_name']
        
    def get_10days_forecast_table(self):
        return self.config['lstm10_table_name']
    
    def get_30days_forecast_table(self):
        return self.config['lstm30_table_name']

    """"""""""""""""""""""""""" Forecast Breakdown Ingredient """""""""""""""""""""""""""
    
    def get_lookup_table(self):
        return self.config['lookup_ingredients_table_name']
        
    """"""""""""""""""""""""""" Redundancy """""""""""""""""""""""""""

    def get_dollartranscount_columns(self):
        return self.config['dollartranscount_table_schema'].split(',')

    def get_itemcount_columns(self):
        return self.config['itemcount_table_schema'].split(',')

    def get_ingredient_columns(self):
        return self.config['ingredient_table_schema'].split(',')

    def get_transcount_upload_table(self):
        return self.config['transcount_upload_table_name']

    def get_itemcount_upload_table(self):
        return self.config['itemcount_upload_table_name']

    def get_dollarsales_upload_table(self):
        return self.config['dollarsales_upload_table_name']

    def get_ingredient_upload_table(self):
        return self.config['ingredient_upload_table_name']

    def get_transcount_upload_key(self,store_num):
        return self.config['transcount_upload_key'].replace('--',store_num)

    def get_itemcount_upload_key(self,store_num):
        return self.config['itemcount_upload_key'].replace('--',store_num)

    def get_dollarsales_upload_key(self,store_num):
        return self.config['dollarsales_upload_key'].replace('--',store_num)

    def get_ingredient_upload_key(self,store_num):
        return self.config['ingredient_upload_key'].replace('--',store_num)
    
    def get_itemcount_upload_key_stg(self,store_num):
        return self.config['itemcount_upload_key_stg'].replace('--',store_num)

    def get_dollarsales_upload_key_stg(self,store_num):
        return self.config['dollarsales_upload_key_stg'].replace('--',store_num)

    def get_ingredient_upload_key_stg(self,store_num):
        return self.config['ingredient_upload_key_stg'].replace('--',store_num)

    def get_dollarsales_ratio_table(self):
        return self.config['dollarsales_ratio_table_name']

    def get_transcount_ratio_table(self):
        return self.config['transcount_ratio_table_name']

    def get_dollarsales_forecast_table(self):
        return self.config['dollarsales_forecast_table_name']

    def get_transcount_forecast_table(self):
        return self.config['transcount_forecast_table_name']
    
    def get_transcount_local_path(self):
        return self.config['transcount_local_path']

    def get_itemcount_local_path(self):
        return self.config['itemcount_local_path']
    
    def get_itemcount_local_path_stg(self):
        return self.config['itemcount_local_path_stg']

    def get_dollarsales_local_path(self):
        return self.config['dollarsales_local_path']
    
    def get_dollarsales_local_path_stg(self):
        return self.config['dollarsales_local_path_stg']

    def get_ingredient_local_path(self):
        return self.config['ingredient_local_path']
    
    def get_ingredient_local_path_stg(self):
        return self.config['ingredient_local_path_stg']
    
    def get_itemcount_function_name(self):
        return self.config['lambda_function_itemcount_name']
    
    def get_test_bucket(self):
        return self.config['test_bucket']
        
    """"""""""""""""""""""""""" Truncate """""""""""""""""""""""""""

    def get_itemqc_database(self):
        return self.config['itemqc_database']

    def get_itemqc_table(self):
        return self.config['itemqc_table_name']

    def get_forecasted_products_database(self):
        return self.config['forecasted_products_database']

    def get_forecasted_products_table(self):
        return self.config['forecasted_products_table_name']

    def get_lookup_database(self):
        return self.config['lookup_database']

    def get_lookup_ingredients_history_table(self):
        return self.config['lookup_ingredients_history_table']

    def get_location_pq_history_table(self):
        return self.config['location_pq_history_table']
    
    def get_baseline_dates_history_table(self):
         return self.config['baseline_dates_history_table']

    def get_baseline_dates_table(self):
         return self.config['baseline_dates_table']

    def get_baseline_database(self):
         return self.config['baseline_database']

    def get_location_pq_table(self):
        return self.config['location_pq_table']

    def get_ratio_dates_table(self):
        return self.config['ratio_dates_table']

    def get_ratio_database(self):
        return self.config['ratio_database'] 

    def get_fixed_crawler_table(self):
        return self.config['fixed_crawler_table']

    #def get_lookup_table(self):
    #    return self.config['lookup_table_name']
    
    """"""""""""""""""""""""""" Dataprep QC """""""""""""""""""""""""""
    
    def get_dollartranscount_local_path(self):
        return self.config['dollartranscount_local_path']
    
    def get_dollartranscount_upload_key(self, store_num):
        return self.config['dollartranscount_upload_key'].replace('--',str(store_num))
        
    def get_dollartranscount_qc_upload_table(self):
        return self.config['dollartranscount_qc_upload_table']

    def get_itemcount_qc_upload_table(self):
        return self.config['itemcount_qc_upload_table']

    def get_batch_size(self):
        return self.config['batch_size']
        
    """"""""""""""""""""""""""" Forecast QC """""""""""""""""""""""""""

    def get_daily_dollartranscount_10days_upload_table(self):
        return self.config['daily_dollartranscount_10days_upload_table_name']

    def get_daily_dollar_10days_table(self):
        return self.config['daily_dollar_10days_table_name']

    def get_daily_transcount_10days_table(self):
        return self.config['daily_transcount_10days_table_name']

    def get_daily_dollartranscount_30days_upload_table(self):
        return self.config['daily_dollartranscount_30days_upload_table_name']

    def get_daily_dollar_30days_table(self):
        return self.config['daily_dollar_30days_table_name']

    def get_daily_transcount_30days_table(self):
        return self.config['daily_transcount_30days_table_name']

    def get_15min_dollartranscount_10days_upload_table(self):
        return self.config['15min_dollartranscount_10days_upload_table_name']

    def get_15min_dollar_10days_table(self):
        return self.config['15min_dollar_10days_table_name']

    def get_15min_transcount_10days_table(self):
        return self.config['15min_transcount_10days_table_name']

    def get_15min_dollartranscount_30days_upload_table(self):
        return self.config['15min_dollartranscount_30days_upload_table_name']

    def get_15min_dollar_30days_table(self):
        return self.config['15min_dollar_30days_table_name']

    def get_15min_transcount_30days_table(self):
        return self.config['15min_transcount_30days_table_name']

    def get_daily_itemcount_10days_upload_table(self):
        return self.config['daily_itemcount_10days_upload_table_name']

    def get_daily_itemcount_10days_table(self):
        return self.config['daily_itemcount_10days_table_name']

    def get_daily_itemcount_30days_upload_table(self):
        return self.config['daily_itemcount_30days_upload_table_name']

    def get_daily_itemcount_30days_table(self):
        return self.config['daily_itemcount_30days_table_name']

    def get_15min_itemcount_10days_upload_table(self):
        return self.config['15min_itemcount_10days_upload_table_name']

    def get_15min_itemcount_10days_table(self):
        return self.config['15min_itemcount_10days_table_name']

    def get_15min_itemcount_30days_upload_table(self):
        return self.config['15min_itemcount_30days_upload_table_name']

    def get_15min_itemcount_30days_table(self):
        return self.config['15min_itemcount_30days_table_name']
    
    def get_10day_forecast_from(self):
        return self.config['10day_forecast_from']
        
    def get_10day_forecast_to(self):
        return self.config['10day_forecast_to']
        
    def get_30day_forecast_from(self):
        return self.config['30day_forecast_from']
        
    def get_30day_forecast_to(self):
        return self.config['30day_forecast_to']
        
    def get_daily_dollartranscount_10days_upload_key(self, store_num):
        return self.config['daily_dollartranscount_10days_upload_key'].replace('--', store_num)
        
    def get_daily_dollartranscount_30days_upload_key(self, store_num):
        return self.config['daily_dollartranscount_30days_upload_key'].replace('--', store_num)
        
    def get_15min_dollartranscount_10days_upload_key(self, store_num):
        return self.config['15min_dollartranscount_10days_upload_key'].replace('--', store_num)
        
    def get_15min_dollartranscount_30days_upload_key(self, store_num):
        return self.config['15min_dollartranscount_30days_upload_key'].replace('--', store_num)
        
    def get_daily_itemcount_10days_upload_key(self, store_num):
        return self.config['daily_itemcount_10days_upload_key'].replace('--', store_num)
        
    def get_daily_itemcount_30days_upload_key(self, store_num):
        return self.config['daily_itemcount_30days_upload_key'].replace('--', store_num)
        
    def get_15min_itemcount_10days_upload_key(self, store_num):
        return self.config['15min_itemcount_10days_upload_key'].replace('--', store_num)
        
    def get_15min_itemcount_30days_upload_key(self, store_num):
        return self.config['15min_itemcount_30days_upload_key'].replace('--', store_num)

    """"""""""""""""""""""""""" Redundancy QC """""""""""""""""""""""""""

    def get_redundancy_forecast_from(self):
        return self.config['redundancy_forecast_from']

    def get_redundancy_forecast_to(self):
        return self.config['redundancy_forecast_to']

    def get_daily_dollartranscount_redundancy_upload_table(self):
        return self.config['daily_dollartranscount_redundancy_upload_table_name']

    def get_daily_dollar_redundancy_table(self):
        return self.config['daily_dollar_redundancy_table_name']

    def get_daily_transcount_redundancy_table(self):
        return self.config['daily_transcount_redundancy_table_name']

    def get_15min_dollartranscount_redundancy_upload_table(self):
        return self.config['15min_dollartranscount_redundancy_upload_table_name']

    def get_15min_dollar_redundancy_table(self):
        return self.config['15min_dollar_redundancy_table_name']

    def get_15min_transcount_redundancy_table(self):
        return self.config['15min_transcount_redundancy_table_name']

    def get_daily_itemcount_redundancy_upload_table(self):
        return self.config['daily_itemcount_redundancy_upload_table_name']

    def get_daily_itemcount_redundancy_table(self):
        return self.config['daily_itemcount_redundancy_table_name']

    def get_15min_itemcount_redundancy_upload_table(self):
        return self.config['15min_itemcount_redundancy_upload_table_name']

    def get_15min_itemcount_redundancy_table(self):
        return self.config['15min_itemcount_redundancy_table_name']
    
    def get_daily_dollartranscount_upload_key(self, store):
        return self.config['daily_dollartranscount_upload_key'].replace('--', store)
    
    def get_15min_dollartranscount_upload_key(self, store):
        return self.config['15min_dollartranscount_upload_key'].replace('--', store)
    
    def get_daily_itemcount_upload_key(self, store):
        return self.config['daily_itemcount_upload_key'].replace('--', store)
    
    def get_15min_itemcount_upload_key(self, store):
        return self.config['15min_itemcount_upload_key'].replace('--', store)

    """""""""""""""""""""CSV to JSON 10 Days ahead"""""""""""""""""""""""
    
    def get_forecast_type(self):
        return self.config['forecast_type']

    def get_json_bucket(self):
        return self.config['json_bucket']

    def get_dollarsales_weekday_table(self):
        return self.config['dollarsales_weekday_table_name']
    
    def get_transcount_weekday_table(self):
        return self.config['transcount_weekday_table_name']
    
    def get_itemcount_weekday_table(self):
        return self.config['itemcount_weekday_table_name']
    
    def get_ingredient_weekday_table(self):
        return self.config['ingredient_weekday_table_name']
    
    def get_weekly_stats_column(self):
        return self.config['weekly_stats_table_schema'].split(',')
    
    def get_json_local_path(self):
        return self.config['json_local_path']
    
    def get_database(self):
        return self.config['database']

    def get_baseline_database(self):
        return self.config['baseline_database']
    
    def get_15min_table(self):
        return self.config['15min_table_name']
    
    def get_15min_dollar_table(self):
        return self.config['15min_dollar_table_name']
        
    def get_15min_transcount_table(self):
        return self.config['15min_transcount_table_name']
    
    def get_daily_final_table(self):
        return self.config['daily_final_table_name']
    
    def get_15min_final_table(self):
        return self.config['15min_final_table_name']
    
    def get_daily_dollartranscount_columns(self):
        return self.config['daily_dollartranscount_table_schema'].split(',')
        
    def get_to_str_daily_dollartranscount_columns(self):
        return self.config['to_str_daily_dollartranscount_column_names'].split(',')
    
    def get_15min_dollartranscount_columns(self):
        return self.config['15min_dollartranscount_table_schema'].split(',')
    
    def get_to_str_15min_dollartranscount_columns(self):
        return self.config['to_str_15min_dollartranscount_column_names'].split(',')
    
    def get_15min_itemcount_table(self):
        return self.config['15min_itemcount_table_name']
    
    def get_15min_itemcount_columns(self):
        return self.config['15min_itemcount_table_schema'].split(',')
    
    def get_to_str_15min_itemcount_columns(self):
        return self.config['to_str_15min_itemcount_column_names'].split(',')
    
    def get_15min_ingredient_table(self):
        return self.config['15min_ingredient_table_name']
    
    def get_15min_ingredient_columns(self):
        return self.config['15min_ingredient_table_schema'].split(',')
    
    def get_to_str_15min_ingredient_columns(self):
        return self.config['to_str_15min_ingredient_column_names'].split(',')
    
    def get_local_path(self):
        return self.config['local_path']
    
    def get_upload_path(self):
        return self.config['upload_path']
    
    def get_alert_bucket(self):
        return self.config['alert_upload_bucket']
    
    def get_alert_path(self):
        return self.config['alert_upload_path']
        
    #30days
    def get_summary_upload_path(self):
        return self.config['summaryjson_upload_path']
    
    def get_summary_upload_path_test(self):
        return self.config['summaryjson_upload_path_test']
        
    def get_summaryjson_local_path(self):
        return self.config['summaryjson_local_path']
        
    """"""""" list of jsons qc """""""""

    def get_total_days(self):
        return self.config['total_days']

    def get_no_of_days_ahead(self):
        return self.config['no_of_days_ahead']

    def get_upload_key(self):
        return self.config['upload_key']

    def get_slack_channel(self):
        return self.config['slack_channel']

    def get_hook_url(self):
        return self.config['hook_url']

    def get_slack_username(self):
        return self.config['slack_username']

    def get_slack_emoji(self):
        return self.config['slack_emoji']

    def get_json_prefix(self):
        return self.config['json_prefix']

    def get_success_message(self):
        return self.config['success_message']

    def get_failure_message(self):
        return self.config['failure_message']
        
    def get_store_missing_message(self):
        return self.config['store_missing_message']

    def get_date_missing_message(self):
        return self.config['date_missing_message']
    
    def get_cross_account_arn(self):
        return self.config['cross_account_arn']
        
    def get_role_session_name(self):
        return self.config['role_session_name']
            
    ######### Initial Itemcount to S3 ########
    
    def get_itemhistory_15min_key(self,store_num,date):
        return self.config['15min_key'].replace('__store_num__',store_num).replace('__date__',str(date))

    def get_itemhistory_daily_key(self,store_num,date):
        return self.config['daily_key'].replace('__store_num__',store_num).replace('__date__',str(date))
    
    ######## Itemcount Athena History ########
    
    def get_output_s3key(self, store_num):
        return self.config['output_s3key'].replace('__store_num__',store_num)
    
    ######### Forecast QC additional changes ######

    def get_place_holder_daily(self):
        return self.config['place_holder_daily']

    def get_place_holder_15min(self):
        return self.config['place_holder_15min']
    
    def get_secret_key_slack(self):
        return self.config['secret_key_slack']
     
    ##########QC SLACK################

    def get_slack_channel_QC(self):
        return self.config['slack_channel_QC']

    def get_slack_message_dataprep(self):
        return self.config['slack_message_dataprep']

    def get_qc_query_dataprep(self):
        return self.config['qc_query_dataprep']

    def get_qc_jobs_dataprep(self):
        return self.config['qc_jobs_dataprep']

    def get_slack_notification_lambda(self):
        return self.config['slack_notification_lambda']

    ##########Truncate Slack############
    
    def get_truncate_slack_upload_key(self):
        return self.config['truncate_slack_upload_key']

    ######datalke initial slack#####

    def get_datalake_initial_slack_upload_msg_key(self):
        return self.config['datalake_initial_slack_upload_msg_key']

    def get_datalake_initial_slack_upload_crawler_path(self):
        return self.config['datalake_initial_slack_upload_crawler_path']

    def get_local_path_slack_datalake(self):
        return self.config['local_path_slack_datalake']
    
    def get_columns_datalake_initial_slack(self):
        return self.config['columns_datalake_initial_slack'].split(',')

    def get_max_business_date_columns(self):
        return self.config['max_business_date_columns'].split(',')

    def get_local_bd_path(self):
        return self.config['local_bd_path']

    def get_local_bd_item_path(self):
        return self.config['local_bd_item_path']

    def get_upload_max_bd_path(self):
        return self.config['upload_max_bd_path']

    def get_upload_max_item_path(self):
        return self.config['upload_max_item_path']
    ######### datalake initial final slack###########
    
    def get_columns_initial_final_slack(self):
        return self.config['columns_initial_final_slack'].split(',')

    def get_columns_inference_input_crawler(self):
        return self.config['columns_inference_input_crawler'].split(',')

    def get_local_crawler_path(self):
        return self.config['local_crawler_path']

    def get_upload_crawler_path(self):
        return self.config['upload_crawler_path']

    def get_initial_final_slack_upload_msg_key(self):
        return self.config['initial_final_slack_upload_msg_key']

    def get_fixed_crawler_csv_key(self):
        return self.config['fixed_crawler_csv_key']
    
    def get_status_to_run_forecast_for(self):
        return self.config['status_to_run_forecast_for']
    
    def get_local_crawler_path_forecast(self):
        return self.config['local_crawler_path_forecast']

    def get_upload_forecast_crawler_path(self):
        return self.config['upload_forecast_crawler_path']

    #######Forecast QC##############
    
    def get_columns_forecast_qc(self):
        return self.config['columns_forecast_qc'].split(',')
    
    def get_local_path_forecast_qc(self):
        return self.config['local_path_forecast_qc']

    def get_upload_key_forecast_qc(self):
        return self.config['upload_key_forecast_qc']

    def get_forecast_qc_slack_upload_msg_key(self):
        return self.config['forecast_qc_slack_upload_msg_key']

    def get_baseline_qc_slack_upload_msg_key(self):
        return self.config['baseline_qc_slack_upload_msg_key']

   ###### Load data daily forecast#######
    
    def get_daily_dollars_14day_key(self,store_num):
        return self.config['daily_dollars_14day_key'].replace('--',store_num)
    
    def get_daily_dollars_30day_key(self,store_num):
        return self.config['daily_dollars_30day_key'].replace('--',store_num)

    def get_daily_trans_14day_key(self,store_num):
        return self.config['daily_trans_14day_key'].replace('--',store_num)
    
    def get_daily_trans_30day_key(self,store_num):
        return self.config['daily_trans_30day_key'].replace('--',store_num)

    def get_daily_item_30day_key(self,store_num):
        return self.config['daily_item_30day_key'].replace('--',store_num)

    def get_daily_item_14day_key(self,store_num):
        return self.config['daily_item_14day_key'].replace('--',store_num)

    def get_daily_dollars_redundancy_key(self,store_num):
        return self.config['daily_dollars_redundancy_key'].replace('--',store_num)

    def get_daily_trans_redundancy_key(self,store_num):
        return self.config['daily_trans_redundancy_key'].replace('--',store_num)

    def get_daily_item_redundancy_key(self,store_num):
        return self.config['daily_item_redundancy_key'].replace('--',store_num)

    def get_ten_days_upload_table_name_dollars_10(self):
        return self.config['ten_days_upload_table_name_dollars_10']
    
    def get_twenty_days_upload_table_name_dollars_30(self):
        return self.config['twenty_days_upload_table_name_dollars_30']

    def get_thirty_days_upload_table_name_dollars_red(self):
        return self.config['thirty_days_upload_table_name_dollars_red']

    def get_ten_days_upload_table_name_trans_10(self):
        return self.config['ten_days_upload_table_name_trans_10']

    def get_twenty_days_upload_table_name_trans_30(self):
        return self.config['twenty_days_upload_table_name_trans_30']

    def get_thirty_days_upload_table_name_trans_red(self):
        return self.config['thirty_days_upload_table_name_trans_red']

    def get_ten_days_upload_table_name_item_10(self):
        return self.config['ten_days_upload_table_name_item_10']

    def get_twenty_days_upload_table_name_item_30(self):
        return self.config['twenty_days_upload_table_name_item_30']


    def get_thirty_days_upload_table_name_item_red(self):
        return self.config['thirty_days_upload_table_name_item_red']


    ####### Tableau UI   ######

    def get_forecast_types(self):
        return self.config['forecast_types']
    
    def get_item_count_query_file(self):
        return self.config['item_count_query_file']

    def get_ingredient_count_query_file(self):
        return self.config['ingredient_count_query_file']

    def get_forecast_query_file(self):
        return self.config['forecast_type_query_file']

    def get_forecast_start_date_file(self):
        return self.config['forecast_start_date_query_file']

    def get_dollarsales_transcount_query_file(self):
        return self.config['dollarsales_transcount_query_file']
    
    def get_forecast_table(self):
        return self.config['forecast_table']
    
    def get_item_count_14_days(self):
        return self.config['item_count_14_days']

    def get_item_count_30_days(self):
        return self.config['item_count_30_days']

    def get_item_count_arima(self):
        return self.config['item_count_arima']
    
    def get_item_count_redundancy(self):
        return self.config['item_count_redundancy']

    def get_ingredient_count_14_days(self):
        return self.config['ingredient_count_14_days']

    def get_ingredient_count_30_days(self):
        return self.config['ingredient_count_30_days']

    def get_ingredient_count_arima(self):
        return self.config['ingredient_count_arima']
    
    def get_ingredient_count_redundancy(self):
        return self.config['ingredient_count_redundancy']

    def get_item_count_column_names(self):
        return self.config['item_count_columns']
        
    def get_ingredient_count_column_names(self):
        return self.config['ingredients_columns']

    def get_dollarsales_transcount_column_names(self):
        return self.config['dollarsales_transcount_columns']

    def get_location_pq_columns(self):
        return self.config['location_pq_columns']

    def get_location_pq_columns_names(self):
        return self.config['location_pq_column_names']

    def get_local_path_item_file(self):
        return self.config['item_file_local_path']

    def get_local_path_ingredient_file(self):
        return self.config['ingredient_file_local_path']
    
    def get_local_path_dollarsales_file(self):
        return self.config['dollarsales_file_local_path']

    def get_location_pq_file_path(self):
        return self.config['location_pq_file_path']

    def get_upload_path_item_file(self):
        return self.config['item_file_upload_path']

    def get_upload_path_ingredient_file(self):
        return self.config['ingredient_file_upload_path']
    
    def get_upload_path_item_archival_file(self):
        return self.config['item_file_upload_path_archival']
    
    def get_upload_path_ingredient_archival_file(self):
        return self.config['ingredient_file_upload_path_archival']

    def get_upload_path_dollarsales_file(self):
        return self.config['dollarsales_file_upload_path']
    
    #####Archival Daily data to Monthly- Parquet #####

    def get_staging_path_dollars_daily(self, store_num):
        return self.config['staging_path_dollars_daily'].replace('--',store_num)

    def get_staging_path_dollars_15min(self, store_num):
        return self.config['staging_path_dollars_15min'].replace('--',store_num)

    def get_staging_path_item_daily(self, store_num):
        return self.config['staging_path_item_daily'].replace('--',store_num)

    def get_staging_path_item_15min(self, store_num):
        return self.config['staging_path_item_15min'].replace('--',store_num)

    def get_archival_path_dollars_daily(self, store_num):
        return self.config['archival_path_dollars_daily'].replace('--',store_num)

    def get_archival_path_dollars_15min(self, store_num):
        return self.config['archival_path_dollars_15min'].replace('--',store_num)

    def get_archival_path_item_daily(self, store_num):
        return self.config['archival_path_item_daily'].replace('--',store_num)

    def get_archival_path_item_15min(self, store_num):
        return self.config['archival_path_item_15min'].replace('--',store_num)
    ####Local and upload path initial and final Archival#######
    
    def get_local_path_final_DS_daily_failed_stores(self, store_num):
        return self.config['local_path_final_DS_daily_failed_stores']
    
    def get_local_path_final_DS_15min_failed_stores(self, store_num):
        return self.config['local_path_final_DS_15min_failed_stores']
    
    def get_local_path_final_IC_daily_failed_stores(self, store_num):
        return self.config['local_path_final_IC_daily_failed_stores']
    
    def get_local_path_final_IC_15min_failed_stores(self, store_num):
        return self.config['local_path_final_IC_15min_failed_stores']
    
    def get_upload_path_final_DS_daily_failed_stores(self, store_num):
        return self.config['upload_path_final_DS_daily_failed_stores'].replace('--',store_num)
    
    def get_upload_path_final_DS_15min_failed_stores(self, store_num):
        return self.config['upload_path_final_DS_15min_failed_stores'].replace('--',store_num)
    
    def get_upload_path_final_IC_daily_failed_stores(self, store_num):
        return self.config['upload_path_final_IC_daily_failed_stores'].replace('--',store_num)
    
    def get_upload_path_final_IC_15min_failed_stores(self, store_num):
        return self.config['upload_path_final_IC_15min_failed_stores'].replace('--',store_num)
    
    def get_local_path_initial_DS_daily_failed_stores(self, store_num):
        return self.config['local_path_initial_DS_daily_failed_stores']
    
    def get_local_path_initial_DS_15min_failed_stores(self, store_num):
        return self.config['local_path_initial_DS_15min_failed_stores']
    
    def get_local_path_initial_IC_daily_failed_stores(self, store_num):
        return self.config['local_path_initial_IC_daily_failed_stores']
    
    def get_local_path_initial_IC_15min_failed_stores(self, store_num):
        return self.config['local_path_initial_IC_15min_failed_stores']
    
    def get_upload_path_initial_DS_daily_failed_stores(self, store_num):
        return self.config['upload_path_initial_DS_daily_failed_stores'].replace('--',store_num)
    
    def get_upload_path_initial_DS_15min_failed_stores(self, store_num):
        return self.config['upload_path_initial_DS_15min_failed_stores'].replace('--',store_num)
    
    def get_upload_path_initial_IC_daily_failed_stores(self, store_num):
        return self.config['upload_path_initial_IC_daily_failed_stores'].replace('--',store_num)
    
    def get_upload_path_initial_IC_15min_failed_stores(self, store_num):
        return self.config['upload_path_initial_IC_15min_failed_stores'].replace('--',store_num)
    ##
    
    def get_staging_path_dollars_daily_initial(self, store_num):
        return self.config['staging_path_dollars_daily_initial'].replace('--',store_num)

    def get_staging_path_dollars_15min_initial(self, store_num):
        return self.config['staging_path_dollars_15min_initial'].replace('--',store_num)

    def get_staging_path_item_daily_initial(self, store_num):
        return self.config['staging_path_item_daily_initial'].replace('--',store_num)

    def get_staging_path_item_15min_initial(self, store_num):
        return self.config['staging_path_item_15min_initial'].replace('--',store_num)

    def get_archival_path_dollars_daily_initial(self, store_num):
        return self.config['archival_path_dollars_daily_initial'].replace('--',store_num)

    def get_archival_path_dollars_15min_initial(self, store_num):
        return self.config['archival_path_dollars_15min_initial'].replace('--',store_num)

    def get_archival_path_item_daily_initial(self, store_num):
        return self.config['archival_path_item_daily_initial'].replace('--',store_num)

    def get_archival_path_item_15min_initial(self, store_num):
        return self.config['archival_path_item_15min_initial'].replace('--',store_num)
    
    ##Data-layer daily -Qc##
    
    def get_temp_upload_key(self):
        return self.config['temp_upload_key']
    
    def get_datalake_initial_slack_upload_crawler_path_remod(self):
        return self.config['datalake_initial_slack_upload_crawler_path_remod']
    
    ##Remodeling changes
    
    def get_item_remodeling_daily_function_name(self):
        return self.config['lambda_function_name_item_daily']
    
    def get_item_remodeling_15min_function_name(self):
        return self.config['lambda_function_name_item_15min']
    
    def get_dollars_remodeling_daily_function_name(self):
        return self.config['lambda_function_name_dollars']
    
    def get_remodeling_slack_local_path(self):
        return self.config['remodeling_slack_local_key']
    
    def get_remodeling_slack_key(self):
        return self.config['remodeling_slack_key']
    
    def get_15min_key_dollars(self,store_num):
        return self.config['15min_key_dollars'].replace('--',store_num)

    def get_daily_key_dollars(self,store_num):
        return self.config['daily_key_dollars'].replace('--',store_num)
    
    def get_daily_key_staging_dollars(self,store_num):
        return self.config['daily_key_staging_dollars'].replace('--',store_num)

    def get_15min_key_staging_dollars(self,store_num):
        return self.config['15min_key_staging_dollars'].replace('--',store_num)
    
    def get_15min_table_dollars(self):
        return self.config['15min_table_name_dollars']
    
    def get_daily_table_dollars(self):
        return self.config['daily_table_name_dollars']
    
    def get_remodeling_stores_columns(self):
        return self.config['remodeling_stores_columns']
    
    def get_daily_columns_dollars(self):
        return self.config['daily_table_schema_dollars'].split(',')
    
    def get_15min_columns_dollars(self):
        return self.config['15min_table_schema_dollars'].split(',')
