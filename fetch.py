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

    response = requests.post(url='https://auth.anaplan.com/token/authenticate',
                             data={}, verify=False, auth=HTTPBasicAuth(user_name, pword))
    response_dict = j.loads(response.text)
    this_token = response_dict['tokenInfo']

    return this_token

def fetch_models(token):

    response = requests.get(url='https://api.anaplan.com/2/0/models',
                            verify=False, headers={'authorization': 'AnaplanAuthToken '+token})

    return response

def fetch_modules(modelid, token):

    response = requests.get(url=f'https://api.anaplan.com/2/0/models/{modelid}/modules', verify=False, headers={
                            'authorization': 'AnaplanAuthToken '+token, 'Accept': 'application/json'})

    return response


def fetch_model_views(modelid, token):

    response = requests.get(url=f'https://api.anaplan.com/2/0/models/{modelid}/views', verify=False, headers={
                            'authorization': 'AnaplanAuthToken '+token, 'Accept': 'application/json'})

    return response


def fetch_module_views(modelid, moduleid, token):

    response = requests.get(url=f'https://api.anaplan.com/2/0/models/{modelid}/modules/{moduleid}/views', verify=False, headers={
                            'authorization': 'AnaplanAuthToken '+token, 'Accept': 'application/json'})

    return response


def fetch_lineitems(modelid, token):

    # /models/{modelId}/lineItems

    response = requests.get(url=f'https://api.anaplan.com/2/0/models/{modelid}/lineItems', verify=False, headers={
                            'authorization': 'AnaplanAuthToken '+token})

    return response


def fetch_processdefinitions(workspaceid, modelid, token):

    # /models/{modelId}/lineItems

    response = requests.get(url=f'https://api.anaplan.com/2/0/workspaces/{workspaceid}/models/{modelid}/processes', verify=False, headers={
                            'authorization': 'AnaplanAuthToken '+token})

    return response


def fetch_view_dimensions(modelid, viewid, token):

    # /models/{modelId}/lineItems

    response = requests.get(url=f'https://api.anaplan.com/2/0/models/{modelid}/views/{viewid}', verify=False, headers={
                            'authorization': 'AnaplanAuthToken '+token})

    return response


def fetch_cell_data_for_view(modelid, viewid, token):

    response = requests.get(url=f'https://api.anaplan.com/2/0/models/{modelid}/views/{viewid}/data', verify=False, headers={
                            'authorization': 'AnaplanAuthToken '+token})

    return response


def fetch_all_line_item_ids_for_model_module(modelid, moduleid, token):

    response = requests.get(url=f'https://api.anaplan.com/2/0//models/{modelid}/modules/{moduleid}/lineItems', verify=False, headers={
                            'authorization': 'AnaplanAuthToken '+token})

    return response


def fetch_process_metadata(processid, modelid, token):

    # /models/{modelId}/processes/{processId}

    response = requests.get(url=f'https://api.anaplan.com/2/0//models/{modelid}/processes/{processid}', verify=False, headers={
                            'authorization': 'AnaplanAuthToken '+token})

    return response


def start_process(workspaceid, modelid, processid, token):

    # /workspaces/{workspaceId}/models/{modelId}/processes/{processId}/tasks

    h = {
        'Content-Type': 'application/json',
        'authorization': 'AnaplanAuthToken '+token
    }

    d = j.dumps({'localeName': 'en_US'})

    response = requests.post(
        url=f'https://api.anaplan.com/2/0/workspaces/{workspaceid}/models/{modelid}/processes/{processid}/tasks', verify=False, headers=h, data=d)

    return response

def monitor_task(workspaceid,modelid,processid,taskid,token):
    
    # /workspaces/{workspaceId}/models/{modelId}/processes/{processId}/tasks/{taskId}

    response = requests.get(url=f'https://api.anaplan.com/2/0/workspaces/{workspaceid}/models/{modelid}/processes/{processid}/tasks/{taskid}', verify=False, headers={'authorization': 'AnaplanAuthToken '+token})

    return response

def query_chunks_in_dump_file(workspaceid,modelid,processid,taskid,objectid,token):
    
    # /workspaces/{workspaceId}/models/{modelId}/processes/{processId}/tasks/{taskId}/dumps/{objectId}

    response = requests.get(url=f'https://api.anaplan.com/2/0/workspaces/{workspaceid}/models/{modelid}/processes/{processid}/tasks/{taskid}/dumps/{objectid}', verify=False, headers={'authorization': 'AnaplanAuthToken '+token})

    return response

def query_chunks_in_export_file(workspaceid,modelid,fileid,token):
    
    # /workspaces/{workspaceId}/models/{modelId}/files/{fileId}/chunks

    response = requests.get(url=f'https://api.anaplan.com/2/0/workspaces/{workspaceid}/models/{modelid}/files/{fileid}/chunks', verify=False, headers={'authorization': 'AnaplanAuthToken '+token})

    return response

def get_export_file_list(workspaceid,modelid,token):

    response = requests.get(url=f'https://api.anaplan.com/2/0/workspaces/{workspaceid}/models/{modelid}/files', verify=False, headers={'authorization': 'AnaplanAuthToken '+token})

    return response

def get_chunk(workspaceid,modelid,fileid,chunkid,token):

    # /workspaces/{workspaceId}/models/{modelId}/files/{fileId}/chunks/{chunkId

    response = requests.get(url=f'https://api.anaplan.com/2/0/workspaces/{workspaceid}/models/{modelid}/files/{fileid}/chunks/{chunkid}', verify=False, headers={'authorization': 'AnaplanAuthToken '+token})

    return response    

def output_metadata():

    user = os.environ['ana_user_name']
    anapword = os.environ['ana_pwd']

    tokenset = fetch_token(
        user_name=user,
        pword=anapword)

    del_outputs()

    model_response = fetch_models(tokenset['tokenValue'])
    model_list = j.loads(model_response.text)
    model_df = pd.DataFrame(model_list['models'])
    model_df.to_csv(f'./outputs/models.csv', header=True, sep='|', index=False)

    for m in model_list['models']:

        modelid = m['id']
        workspaceid = m['currentWorkspaceId']

        modelname = m['name'].replace('.', '').replace(' ', '')
        modules_response = fetch_modules(
            modelid=modelid, token=tokenset['tokenValue'])
        modules = j.loads(modules_response.text)
        module_df = pd.DataFrame(modules['modules'])
        module_df.to_csv(
            f'./outputs/{modelname}_modules.csv', header=True, sep='|', index=False)

        processes_response = fetch_processdefinitions(
            workspaceid=workspaceid, modelid=modelid, token=tokenset['tokenValue'])
        processes_set = j.loads(processes_response.text)
        processes_df = pd.DataFrame(processes_set['processes'])
        processes_df.to_csv(f'./outputs/processes.csv',
                            header=True, sep='|', index=False)


def output_specific_process_info(startprocess, taskid=None):

    modelid = 'B3B98B29A95143B4841FC26A290EA705'
    workspaceid = '8a81b0127122eab90171a6b0a2987646'
    processid = '118000000060'

    user = os.environ['ana_user_name']
    anapword = os.environ['ana_pwd']

    tokenset = fetch_token(
        user_name=user,
        pword=anapword)

    process_metadata_result = fetch_process_metadata(
        processid=processid, modelid=modelid, token=tokenset['tokenValue'])
    process_metadata_set = j.loads(process_metadata_result.text)
    process_metadata_df = pd.DataFrame(
        process_metadata_set['processMetadata']['actions'])
    process_metadata_df.to_csv(
        f'./outputs/process_{processid}_metadata.csv', header=True, sep='|', index=False)

    if startprocess == 1:
        taskid_response = start_process(
            workspaceid=workspaceid, modelid=modelid, processid=processid, token=tokenset['tokenValue'])
        jtaskid = j.loads(taskid_response.text)
        taskid = jtaskid['task']['taskid']
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

        print('/////////////////////////////////////////////////////////////')
        print(file_or_dump_objects)

        # for dump_object in dump_objects:
        #     get_chunks_list = query_chunks_in_dump_file(workspaceid=workspaceid,modelid=modelid,processid=processid,taskid=taskid,objectid=dump_object['objectid'],token=tokenset['tokenValue'])
        #     print(j.loads(get_chunks_list.text))

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

        # file_list_response = get_export_file_list(workspaceid=workspaceid,modelid=modelid,token=tokenset['tokenValue'])
        # print('------------------------------------------------------')
        # jfile_list = j.loads(file_list_response.text)
        # print(jfile_list)

    else:
        print('not complete yet.')
    # start process


# output_metadata()
output_specific_process_info(startprocess=0,taskid='E1CFE6CDF862419196FB107D36EA2113')
