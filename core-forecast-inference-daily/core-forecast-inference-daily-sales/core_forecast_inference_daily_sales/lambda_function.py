import os
import json
from inference_config import Config
from core_forecast_inference_daily_lstm.create_endpoint import lambda_handler as create_endpoint
from core_forecast_inference_daily_lstm.delete_endpoint import lambda_handler as delete_endpoint
from core_forecast_inference_daily_lstm.invoke_endpoint import lambda_handler as invoke_endpoint

def lambda_handler(event, context):
    ENV = os.getenv('ENV')
    
    with open('core_forecast_inference_daily_lstm/config.json') as config_params:
        config_dict = json.load(config_params)[ENV][event['metric']]
        config_dict['location_num'] = event['location_num']
        config_dict['metric'] = event['metric']
        config = Config.from_event(config_dict)

    if event['action'] == 'create_endpoint':
        print("Action is to create endpoint.")
        create_endpoint(config)
    elif event['action'] == 'invoke_endpoint':
        print("Action is to invoke endpoint.")
        invoke_endpoint(config)
    elif event['action'] == 'delete_endpoint':
        print("Action is to delete endpoint.")
        delete_endpoint(config)
    else:
        print("Action not supported.")

# if __name__ == '__main__':
#     event = {
#         "action": "create_endpoint" | "invoke_endpoint" | "delete_endpoint",
#         "metric": "dollarsales" | "transcount" | "itemcount",
#         "location_num": "all"
#     }
#     lambda_handler(event,{})