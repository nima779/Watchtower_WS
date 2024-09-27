from flask import Flask, jsonify, request, jsonify, make_response
from datetime import datetime
import requests
import math
import json
import os
import asyncio
import uuid
import urllib.parse
from concurrent.futures import ThreadPoolExecutor




app = Flask(__name__)


KNACK_HEADERS = {'Content-Type':'application/json','X-Knack-Application-Id': '63702b9fb7752900212c987e','X-Knack-REST-API-KEY': 'ea208c3f-ecea-4b0a-8b04-3f0d1f19168f'}
KNACK_HEADERS_2 = {'Content-Type':'application/json','X-Knack-Application-Id': '63702b9fb7752900212c987e','X-Knack-REST-API-KEY': 'knack'}

SV_GetAssetTypeProperties = "/pages/scene_380/views/view_669/"
SV_GetAssetsForAssetType = "/pages/scene_380/views/view_670/"
SV_GetPropertiesOfAsset = "/pages/scene_378/views/view_676/"
SV_CreateAssetProperty = "/pages/scene_378/views/view_684/"
SV_OBJ_AssetProperty = "object_41"

class AssetTypeProperty():
     def __init__(self):
        self.proptypeid = None
        self.proptype = None
        self.propselectionfield = None

@app.route('/')
def hello_world():
    return 'Hello, World!'

def _build_cors_preflight_response():
    response = make_response()
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add('Access-Control-Allow-Headers', "*")
    response.headers.add('Access-Control-Allow-Methods', "*")
    return response

def _corsify_actual_response(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

def WriteLog(message):
    print(message, flush=True)

# In-memory dictionary to track task statuses
tasks = {}

# ThreadPoolExecutor to run background tasks
executor = ThreadPoolExecutor(max_workers=5)

async def update_asset_type_properties(task_id, asset_type_id):
    WriteLog("[ UPDATE ASSET TYPE PROPERTIES for asset_type_id {} ]".format(task_id, asset_type_id))
    #Get AssetTypeProperties
    r = requests.get("https://api.knack.com/v1" + SV_GetAssetTypeProperties + "records?rows_per_page=1000&view-assettype-details_id=" + asset_type_id, headers=KNACK_HEADERS)
    WriteLog(r.text)
    json_data = json.loads(r.text)
    arrlen = len(json_data['records'])
    WriteLog("-- {} Properties found --".format(arrlen))
    mAssetTypeProperties = []
    mAssetTypeProperties_ids = []
    for i in range(arrlen):
        mAssetTypeProperties_ids.append(json_data['records'][i]['id'])
        mAssetTypeProperties.append(AssetTypeProperty())
        mAssetTypeProperties[i].proptypeid = json_data['records'][i]['id']
        mAssetTypeProperties[i].proptype = json_data['records'][i]['field_702']
        if (json_data['records'][i]['field_703'] != ""): #is selection field
            mAssetTypeProperties[i].propselectionfield = json_data['records'][i]['field_703_raw'][0]['id']
        else:
            mAssetTypeProperties[i].propselectionfield = ""
        WriteLog("Property: {}, Selection Field: {}".format(mAssetTypeProperties[i].proptype, mAssetTypeProperties[i].propselectionfield))
        
    #Get assets for asset type
    r = requests.get("https://api.knack.com/v1" + SV_GetAssetsForAssetType + "records?rows_per_page=1000&view-assettype-details_id=" + asset_type_id, headers=KNACK_HEADERS)
    WriteLog(r.text)
    json_data = json.loads(r.text)
    arrlen = len(json_data['records'])
    WriteLog("-- {} assets found --".format(arrlen))
    for i in range(arrlen):
        perc = i / arrlen * 100
        tasks[task_id] = "Update in progress {}/{} ({}%)".format(i,arrlen,round(perc,0))  # Update task status once done
        mAssetId = json_data['records'][i]['id']
        WriteLog("Asset {}/{}: {}".format(i, arrlen,mAssetId))

        # Get properties of asset
        mfilters = {
            "match": "and",
            "rules": [
                {
                "field": "field_709",
                "operator": "is",
                "value": mAssetId,
                },
            ],
        }
        encoded_filters = urllib.parse.quote(json.dumps(mfilters))
        r = requests.get("https://api.knack.com/v1" + SV_GetPropertiesOfAsset + "records?filters=" + encoded_filters, headers=KNACK_HEADERS)
        WriteLog(r.text)
        json_data2 = json.loads(r.text)
        arrlen2 = len(json_data2['records'])
        WriteLog("-- {} properties found --".format(arrlen2))
        mAssetProperties = []
        mAssetProperties_ids = []
        for j in range(arrlen2):
            mAssetProperties_ids.append(json_data2['records'][j]['field_711_raw'][0]['id'])
            mAssetProperties.append(AssetTypeProperty())
            mAssetProperties[j].proptypeid = json_data2['records'][j]['field_711_raw'][0]['id']
            mAssetProperties[j].proptype = json_data2['records'][j]['field_713']
            if (json_data2['records'][j]['field_719'] != ""): #is selection field
                mAssetProperties[j].propselectionfield = json_data2['records'][j]['field_719_raw'][0]['id']
            else:
                mAssetProperties[j].propselectionfield = ""
            WriteLog("Property: {}, Selection Field: {}".format(mAssetProperties[j].proptype, mAssetProperties[j].propselectionfield))



        # get mAssetProperties which are not in mAssetTypeProperties
        mAssetPropertiesNotinAssetTypeProperties = [x for x in mAssetProperties_ids if x not in mAssetTypeProperties_ids]
        WriteLog("-- {} Properties not in asset types --".format(len(mAssetPropertiesNotinAssetTypeProperties)))
        for m in mAssetPropertiesNotinAssetTypeProperties:
            WriteLog("--- PropertyTypeId {}".format(m))
            #get id
            for k in range(arrlen2):
                if mAssetProperties[k].proptypeid == m:
                    delid = json_data2['records'][k]['id']
                    post_data = {
                        "delete": True
                    }
                    r=requests.delete("https://api.knack.com/v1/objects/" + SV_OBJ_AssetProperty + "/records/" + delid , headers=KNACK_HEADERS, json=post_data)
                    WriteLog(r.text)
                    WriteLog("AssetProperty {} DELETED".format(delid))
                    break

            

        # get mAssetTypeProperties which are not in mAssetProperties
        mAssetTypePropertiesNotinAssetProperties = [x for x in mAssetTypeProperties_ids if x not in mAssetProperties_ids]
        WriteLog("-- {} Properties not in assets --".format(len(mAssetTypePropertiesNotinAssetProperties)))
        for m in mAssetTypePropertiesNotinAssetProperties:
            for kAssetTypeProperty in mAssetTypeProperties:
                if kAssetTypeProperty.proptypeid == m:
                    WriteLog("--- Property: {}, Selection Field: {}".format(kAssetTypeProperty.proptypeid, kAssetTypeProperty.propselectionfield))
                    post_data = {
                        "field_709": mAssetId,
                        "field_710": asset_type_id, 
                        "field_711": kAssetTypeProperty.proptypeid, 
                        "field_713": kAssetTypeProperty.proptype, 
                        "field_719": kAssetTypeProperty.propselectionfield
                    }
                    #WriteLog(post_data)
                    r=requests.post("https://api.knack.com/v1/objects/" + SV_OBJ_AssetProperty + "/records", headers=KNACK_HEADERS, json=post_data)
                    WriteLog(r.text)
                    WriteLog("AssetProperty {} CREATED".format(kAssetTypeProperty.proptypeid))
                    break
        
    tasks[task_id] = "completed"  # Update task status once done

def run_async_update_asset_type_properties(task_id, asset_type_id):
    loop = asyncio.new_event_loop() 
    asyncio.set_event_loop(loop)
    loop.run_until_complete(update_asset_type_properties(task_id, asset_type_id))
    loop.close()

@app.route('/updateAssetTypeProperties/<asset_type_id>', methods=['GET','OPTIONS'])
def trigger_task(asset_type_id):
    if request.method == "OPTIONS": # CORS preflight
        return _build_cors_preflight_response()
    elif request.method == "GET": # The actual request following the preflight
        task_id = str(uuid.uuid4())  # Generate a unique task ID
        tasks[task_id] = "in-progress"  # Mark task as in progress
        executor.submit(run_async_update_asset_type_properties, task_id, asset_type_id)
        response = make_response()
        #add data to response
        response.headers.add('Content-Type', 'application/json')
        response.status_code = 200
        response = jsonify({"message": "Task triggered", "task_id": task_id})
        return _corsify_actual_response(response)

@app.route('/task-status/<task_id>', methods=['GET','OPTIONS'])
def task_status(task_id):
    if request.method == "OPTIONS": # CORS preflight
        return _build_cors_preflight_response()
    elif request.method == "GET": # The actual request following the preflight
        status = tasks.get(task_id, "unknown")  # Get the task status
        response = make_response()
        #add data to response
        response.headers.add('Content-Type', 'application/json')
        response.status_code = 200
        response = jsonify({"task_id": task_id, "status": status})
        return _corsify_actual_response(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

