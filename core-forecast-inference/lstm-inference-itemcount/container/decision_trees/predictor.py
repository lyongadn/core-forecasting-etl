from __future__ import print_function

import os
import json
import pickle
from io import StringIO
import sys
import signal
import traceback
import boto3
import copy
#from multiprocessing import Pool
import csv
import time

import flask
from Run_Inference import Infer
from threading import Thread
import pandas as pd


# The flask app for serving predictions
app = flask.Flask(__name__)
#_pool = Pool(processes=4)

print("test function is loaded in container")

@app.route('/ping', methods=['GET'])
def ping():
    """Determine if the container is working and healthy. In this sample container, we declare
    it healthy if we can load the model successfully."""
    # health = ScoringService.get_model() is not None  # You can insert a health check here

    status = 200 
    return flask.Response(response='\n', status=status, mimetype='application/json')

def runInferences(arguments,dataframe_dict,store):
    
    bucket_name=arguments["bucket_name"]
    athena_csv_filepath=arguments["athena_csv_filepath"]
    mp_path=arguments["mp_path"]
    h5_path=arguments["h5_path"]
    upload_path=arguments["upload_path"]
    target=arguments["target"]
    shard_index=arguments["shard_index"]
    store_num=store

    error_bucket = 'prod-q-forecasting-artifacts'

    print("inside running inferences")
    i=0
    s3_client=boto3.client('s3')
    for data in arguments["combinations"]:
        index=str(shard_index)+"_"+str(i)
        product_num=data
        filename= index+'_'+str(store_num)+'_'+str(product_num)+"_"+str(target)+'_.txt'
        try:
            #store_num=data["location_num"]

            print(product_num)
            #product_num=data["product"]

            dataframe=dataframe_dict[str(product_num)]
            if len(dataframe) < 57:
                pass
            else:
                start = time.time()
                result=Infer.sales_inference(index,str(store_num),product_num,bucket_name,athena_csv_filepath,mp_path,h5_path,upload_path,dataframe)
                print("Inferences running time in seconds : " + str(time.time()-start))

            print("running inference")
            try:
                s3_client.delete_object(Bucket=error_bucket,Key=filename)
                print("Deleted file : " + filename)
            except Exception as exe:
                print("pass")
            i=i+1
        except Exception as e:
            result = "Failure"
            traceback.print_exc()
            print("exception is " + str(e))
            # s3_resource = boto3.resource('s3')
            print("error file created")
            pass
            #with open('/tmp/'+filename,'w') as file:
                #s3_client.upload_fileobj(file, error_bucket, filename)
    Infer.combine_csv("/tmp/","forecast/itemcount/daily/",store_num)
    #Infer.s3toAurora(str(store_num))

def transformation(request_data):
    cfa_bucket = 'prod-q-forecasting-artifacts'

    print("Request Data :"+str(request_data))
    bucket_name=request_data["bucket_name"]
    athena_csv_filepath=request_data["athena_csv_filepath"]
    mp_path=request_data["mp_path"]
    h5_path=request_data["h5_path"]
    upload_path=request_data["upload_path"]
    #index=request_data["index"]
    target=request_data["target"]
    #batch_count=request_data["batch_count"]
    store_num=request_data["store_num"]
    s3_client=boto3.client('s3')

    #client.download_file(cfa_bucket, filepath , '/tmp/Store&product.csv')
    s3_client.download_file(cfa_bucket,'data_lake/Inference/ItemCount/location_num='+str(store_num)+'/Inference_daily.csv','Inference_daily.csv')
    #file_reader= open('/tmp/Store_2206.csv', "r")
    data_csv=pd.read_csv('Inference_daily.csv')
    print("read csv")
    #reader = csv.DictReader(file_reader)
    #data = [row for row in reader]
    dataframe_dict={}
    products=data_csv['product'].unique().tolist()
    for product in products :
        dataframe_dict[str(product)]=data_csv.loc[data_csv['product'] == product]
    
        
    inference_count=len(products)
    print(inference_count)
    print("Inside Invocations")
    print(bucket_name)
    print(athena_csv_filepath)
    print(mp_path)
    print(h5_path)
    print(upload_path)

    data_shards=[]
    data_param_shard=[]

    for i in range(1):
        data_shards.append(products[int((i*inference_count)/1):int(((i+1)*inference_count)/1)])
    j=0
    for shard in data_shards:
        print(len(shard))
        arguments={"bucket_name":bucket_name,"athena_csv_filepath":athena_csv_filepath,"mp_path":mp_path,"h5_path":h5_path,"upload_path":upload_path,"target":target,"combinations":shard,"shard_index":j}
        data_param_shard.append(arguments)
        j=j+1
    #_pool.map(runInferences,data_param_shard)
    print("Inferences started running at : " + str(time.time()))
    runInferences(data_param_shard[0],dataframe_dict,store_num)
    print("Inferences stopped running at : " + str(time.time()))
    
    client = boto3.client('lambda', region_name='us-east-1')
    time.sleep(60)
    json_params ={
		'store_number': store_num,
		'target' : target
		}
    print (json_params)
    while True:
        resp = client.invoke(
                FunctionName= 'Delete_Endpoint_Itemcount-Aurora-Prod',
                InvocationType='Event',
                Payload=json.dumps(json_params)
                )
        if resp['StatusCode']==202:
            print("Success")
            break
        else:
            time.sleep(5)
            continue
    #resp = client.invoke(
    #		FunctionName= 'Delete_Endpoint_Itemcount-Aurora-Prod',
    #		InvocationType='Event',
    #		Payload=json.dumps(json_params)
    #		)


        
      

@app.route('/invocations', methods=['POST'])
def request_receiver():
    status = 202
    request_data = flask.request.json
#     request_data = {
#         "bucket_name": "prod-q-forecasting-artifacts",
#         "athena_csv_filepath": "forecast/lstm/temp_files/",
#         "mp_path": "model-object-retraining-jan21/",
#         "h5_path": "model-object-retraining-jan21/",
#         "upload_path": "forecast/lstm/dollarsales/test_inference/",
#         "target": "03551",
#         "store_num": "03551"
#     }

    request_data_copy=copy.deepcopy(request_data)

    
    print("data received successfully , Data: "+ str(request_data))
    if not request_data:
        status = 400
        return flask.Response(response=json.dumps({'result':'Missing Data'}), status=status, mimetype='application/json')
    t = Thread(target=transformation, args=(request_data_copy, ))
    print (t.isDaemon())
    t.daemon = False
    print (t.isDaemon())
    t.start()
    print("response received!")
    return flask.Response(response=json.dumps({'result':'Received Data'}), status=status, mimetype='application/json')






if __name__=="__main__":
    #_pool = Pool(processes=4)
    app.run('0.0.0.0',port=80,debug=True,use_reloader=False)
    #_pool.join()
    #_pool.close()

