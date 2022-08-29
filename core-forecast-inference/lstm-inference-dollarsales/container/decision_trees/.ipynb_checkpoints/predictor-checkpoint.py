# This is the file that implements a flask server to do inferences. It's the file that you will modify to
# implement the scoring for your own algorithm.

from __future__ import print_function

import os
import json
import pickle
import StringIO
import sys
import signal
import traceback

import flask
from Run_Inference import Infer

# The flask app for serving predictions
app = flask.Flask(__name__)

@app.route('/ping', methods=['GET'])
def ping():
    """Determine if the container is working and healthy. In this sample container, we declare
    it healthy if we can load the model successfully."""
    # health = ScoringService.get_model() is not None  # You can insert a health check here

    status = 200 
    return flask.Response(response='\n', status=status, mimetype='application/json')

@app.route('/invocations', methods=['POST'])
def transformation():
    """Do an inference on a single batch of data. In this sample server, we take data as CSV, convert
    it to a pandas data frame for internal use and then convert the predictions back to CSV (which really
    just means one prediction per line, since there's a single column.
    """
    store_num=flask.request.form.get("store_num")
    bucket_name=flask.request.form.get("bucket_name")
    athena_csv_filepath=flask.request.form.get("athena_csv_filepath")
    mp_path=flask.request.form.get("mp_path")
    h5_path=flask.request.form.get("h5_path")
    upload_path=flask.request.form.get("upload_path")

    print("Inside Invocations")
    print(store_num)
    print(bucket_name)
    print(athena_csv_filepath)
    print(mp_path)
    print(h5_path)
    print(upload_path)


    result=Infer.predict(store_num,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path)

    return flask.Response(response=store_num, status=200, mimetype='application/json')

if __name__=="__main__":
    app.run('0.0.0.0',port=80,debug=True)
