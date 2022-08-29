import json
import boto3

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

    def get_prod_bucket(self):
        return self.config["prod_bucket"]

    def get_location_num(self):
        return self.config["store_num"]

    def get_shorter_forecast_start(self):
        return self.config["shorter_forecast_start"]

    def get_shorter_forecast_end(self):
        return self.config["shorter_forecast_end"]

    def get_longer_forecast_start(self):
        return self.config["longer_forecast_start"]

    def get_longer_forecast_end(self):
        return self.config["longer_forecast_end"]

    def get_redundancy_forecast_start(self):
        return self.config["redundancy_forecast_start"]

    def get_redundancy_forecast_end(self):
        return self.config["redundancy_forecast_end"]

    def get_shorter_forecast_upload_key(self):
        return self.config["shorter_forecast_path"].replace("--location_num--",self.get_location_num())

    def get_longer_forecast_upload_key(self):
        return self.config["longer_forecast_path"].replace("--location_num--",self.get_location_num())

    def get_redundancy_forecast_upload_key(self):
        return self.config["redundancy_forecast_path"].replace("--location_num--",self.get_location_num())

    def get_item_store_combinations_sql_file_path(self):
         return self.config["item_store_combinations_sql_path"]

    def get_holidays_list_sql_file_path(self):
        return self.config["holidays_list_sql_path"]

    def get_sales_data_sql_file_path(self):
        return self.config["sales_data_sql_path"]

    def get_replica_host_link(self):
        return self.config["replica_host_link"]
        
    def get_database(self):
        return self.config["database"]

    def get_secret_key(self):
        return self.config["secret_key"]

    def get_initial_itemlevelcount_table_name(self):
        return self.config["initial_itemlevelcount_table_name"]

    def get_lstm_combinations_table_name(self):
        return self.config["lstm_combinations_table_name"]

    def get_final_itemlevelcount_daily_table_name(self):
        return self.config["final_itemlevelcount_daily_table_name"]

    def get_inferencedates_daily_table_name(self):
        return self.config["inferencedates_daily_table_name"]
