import requests
from requests.auth import HTTPBasicAuth
import json as j
import pandas as pd
import os
import glob

def del_outputs():
    files = glob.glob('./outputs/*')
    for f in files:
        os.remove(f)

def fetch_token(user_name, pword):
    """
    
    collects an authentication token for the API

    """

    response = requests.post(url='https://auth.anaplan.com/token/authenticate',
                             data={}, verify=False, auth=HTTPBasicAuth(user_name, pword))
    response_dict = j.loads(response.text)
    this_token = response_dict['tokenInfo']

    return this_token

def start_process(workspaceid, modelid, processid, token):

    """
    starts the specified process. Will return a taskid that can be monitored

    """


    h = {
        'Content-Type': 'application/json',
        'authorization': 'AnaplanAuthToken '+token
    }

    d = j.dumps({'localeName': 'en_US'})

    response = requests.post(
        url=f'https://api.anaplan.com/2/0/workspaces/{workspaceid}/models/{modelid}/processes/{processid}/tasks', verify=False, headers=h, data=d)

    return response

def monitor_task(workspaceid,modelid,processid,taskid,token):
    
    """
    
    fetches the current status of the provided taskid

    """


    response = requests.get(url=f'https://api.anaplan.com/2/0/workspaces/{workspaceid}/models/{modelid}/processes/{processid}/tasks/{taskid}', verify=False, headers={'authorization': 'AnaplanAuthToken '+token})

    return response

def query_chunks_in_export_file(workspaceid,modelid,fileid,token):
    
    """
    
    collects metadata on the chunks for a specific file id

    """


    response = requests.get(url=f'https://api.anaplan.com/2/0/workspaces/{workspaceid}/models/{modelid}/files/{fileid}/chunks', verify=False, headers={'authorization': 'AnaplanAuthToken '+token})

    return response

def get_chunk(workspaceid,modelid,fileid,chunkid,token):


    """
    
    collects the file chunk for appending to the csv file

    """


    response = requests.get(url=f'https://api.anaplan.com/2/0/workspaces/{workspaceid}/models/{modelid}/files/{fileid}/chunks/{chunkid}', verify=False, headers={'authorization': 'AnaplanAuthToken '+token})

    return response    

def output_specific_process_info(startprocess, taskid=None):
    """ triggers and collects a specific process (see the constants below). Then can be used to monitor the task id it returns.
    Usage: 
    
    1. Set your env variables for the token collection above.
    2. Launch this function with startprocess=1 (no task id) - you will receive a taskid for use in step 3 
    3. Run this function again with startprocess=0 and your new taskid; you may need to run this multiple times as the process may take a while to complete at "their" end.
    4. Once done, you should have files in the outputs folder.
    
    """

    modelid = 'B3B98B29A95143B4841FC26A290EA705'
    workspaceid = '8a81b0127122eab90171a6b0a2987646'
    processid = '118000000060'

    user = os.environ['ana_user_name']
    anapword = os.environ['ana_pwd']

    del_outputs()

    tokenset = fetch_token(
        user_name=user,
        pword=anapword)

    if startprocess == 1:
        taskid_response = start_process(
            workspaceid=workspaceid, modelid=modelid, processid=processid, token=tokenset['tokenValue'])
        jtaskid = j.loads(taskid_response.text)
        taskid = jtaskid['task']['taskId']
        return taskid
    else:
        taskid = taskid

    task_response = monitor_task(workspaceid=workspaceid,modelid=modelid,processid=processid,taskid=taskid,token=tokenset['tokenValue'])
    jtask = j.loads(task_response.text)
    
    file_or_dump_objects = []

    if jtask['task']['currentStep'] == 'Complete.':
        for n in jtask['task']['result']['nestedResults']:
            objectid = n['objectId']
            name = n['details'][0]['values'][5]
            file_or_dump_objects.append({
                'objectid':objectid,
                'objectname':name
            })

        for fobject in file_or_dump_objects:
            chunk_list = query_chunks_in_export_file(workspaceid=workspaceid,modelid=modelid,fileid=fobject['objectid'],token=tokenset['tokenValue'])
            # print(j.loads(chunk_list.text))
            chunks = j.loads(chunk_list.text)['chunks']

            for chunk in chunks:
                chunk_response = get_chunk(workspaceid=workspaceid,modelid=modelid,fileid=objectid,chunkid=chunk['id'],token=tokenset['tokenValue'])
                print('done')
                csv_txt = chunk_response.text
                with open(f"./outputs/{fobject['objectname']}.csv", 'w') as csvFile:
                    csvFile.write(csv_txt)

    else:
        print('not complete yet.')
        print(jtask)
 


# START TASK
# taskid = output_specific_process_info(startprocess=1)
# print(taskid)

# POLL AND COLLECT TASK
output_specific_process_info(startprocess=0,taskid='1E88D93EA3124C25BED66EDC305B9B39')
