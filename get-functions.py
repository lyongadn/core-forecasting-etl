import os
import json
import re
import inspect
import numpy as np
from config import Config

def print_list(lst):
    for i in lst:
        print(i)
    print("Total length:",len(lst))
    print()

def get_info(
        pipeline_folder='core-forecast-slack-alerts', 
        pipeline_name='core-forecast-slack-alert-redundancy-inference-qc', 
        old_pipeline_name='Prod-Aurora-Redundancy-Forecast-QC-SlackNofication', 
        conf_variable_name='conf'):

    script_location = os.path.join(pipeline_folder, pipeline_name, pipeline_name.replace('-','_'))
    script_path = os.path.join(script_location, 'lambda_function.py')
    with open(script_path) as f:
        file_content = f.read()

    matches = re.findall(f'{conf_variable_name}\.([^\W.,(]*)',file_content)
    functions_used = np.unique(matches)

    available,not_callable, no_attribute, loc, config_all_keys = [],[],[],[],[]
    for i in functions_used:
        try:
            class_param = getattr(Config,i)
            if callable(class_param):
                available.append(i)
                loc.append(inspect.getsource(class_param))
            else:
                not_callable.append(i)
        except AttributeError:
            no_attribute.append(i)

    print(f"# of matches: {len(matches)} and # of functions used : {len(functions_used)}")

    print("Available functions: ")
    print_list(available)

    print("Non-callable attributes: ")
    print_list(not_callable)

    print("No attributes: ")
    print_list(no_attribute)

    print("Codes of each available functions: ")
    for fname, code in zip(available,loc):
        print("Function:",fname)
        print(code)
        print("*"*100)

        config_keys = re.findall("self.config\[[\'\"]([^\'\"]*)[\'\"]",code)
        config_all_keys.extend(config_keys)

    config_all_keys = np.unique(config_all_keys)
    print("All required config parameters in config file:")
    print_list(config_all_keys)
    print()

    missing_params_list = []
    with open('config.json') as f:
        config = json.loads(f.read())[old_pipeline_name]
        new_config = {}
        for param in config_all_keys:
            if param in config:
                new_config[param] = config[param]
            else:
                missing_params_list.append(param)
    
        final_config = {
            "dev": new_config,
            "test": {},
            "prod": {}
        }
    print("Final config generated :")
    print(json.dumps(final_config,indent=4))
    print()

    print("Parameters missing from config.json file:")
    print_list(missing_params_list)
    
    init_file_path = os.path.join(script_location, '__init__.py')
    print(f"Creating __init__.py file at {init_file_path}")
    with open(init_file_path,'w'): pass
    
    config_file_path = os.path.join(script_location, 'config.json')
    print(f"Saving config file at : {config_file_path}")
    with open(config_file_path,'w') as f:
        json.dump(final_config,f,indent=4)


if __name__ == '__main__':
    import sys
    # get_info(sys.argv[1])
    get_info(pipeline_folder=sys.argv[1], 
        pipeline_name=sys.argv[2], 
        old_pipeline_name=sys.argv[3], 
        conf_variable_name=sys.argv[4] if len(sys.argv)>4 else 'conf')

#  python get-functions.py core-forecast-slack-alerts core-forecast-slack-alert-redundancy-inference-qc Prod-Aurora-Redundancy-Forecast-QC-SlackNofication conf