import yaml
import sys
from cfn_tools import load_yaml, dump_yaml
import json

if __name__ == "__main__":
    cloudformation_path = f"{sys.argv[1]}"
    step_function_def_path = './core-forecast-baseline-step-function/core_forecast_baseline_step_function/state_machine.asl.json'

    environment = '${Environment}'
    account_id = '${AWS::AccountId}'
    region = '${AWS::Region}'
    account_name = '${CurrentAccountAlias}'

    #Read in Step Function
    with open(step_function_def_path) as step_function_json:
        step_function_string = step_function_json.read()

    #Perform env replacement
    step_function_string = step_function_string.replace('<env>', environment)
    step_function_string = step_function_string.replace('<region>', region)
    step_function_string = step_function_string.replace('<account_id>', account_id)
    step_function_string = step_function_string.replace('<account_name>', account_name)

    #Update Cloudformation
    with open(cloudformation_path) as input_cfn:
        cfn_script = load_yaml(input_cfn)
        print(cfn_script)
        cfn_script['Resources']['StepFunction']['Properties']['DefinitionString'] = {"Fn::Sub": json.dumps(json.loads(step_function_string), indent=2)}
        new_cfn_script = dump_yaml(cfn_script)
    print(f'step function definition is ready; new CFN is {new_cfn_script}')

    #Write Updated Cloudformation
    with open(cloudformation_path, 'w') as output_cfn:
        output_cfn.write(new_cfn_script)