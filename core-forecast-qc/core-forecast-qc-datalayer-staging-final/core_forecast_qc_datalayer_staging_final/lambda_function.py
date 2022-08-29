import sys


def lambda_handler(event, context):

    return {
        "message": "Hello World",
        "event": event
    }


# This block is to run locally on machines when the lambda_function.py file is run. In a true lambda environment this is ignored.
if __name__ == '__main__':
    print(lambda_handler({},{}))