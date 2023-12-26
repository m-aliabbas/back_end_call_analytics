from typing import Union
from fastapi import FastAPI,Response
from fastapi.responses import StreamingResponse
from Processor.Interface import Interface
from Processor.LogInterface import LogInterface
import json
from fastapi import FastAPI, UploadFile, File,WebSocket, Form
from typing import List
import ast
from bson import json_util
from fastapi.middleware.cors import CORSMiddleware
import requests
import time
import os
import zipfile
from pydantic import BaseModel

app = FastAPI()


def parse_json(data):
    return json.loads(json_util.dumps(data))


origins = ["*"]


interface = Interface()
LogInterface = LogInterface()


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/get_full_data")
def get_full_data():
    data=interface.get_complete_data()
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_full_transcripts")
def get_full_transcript():
    data=interface.get_full_transripts()
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_sequences")
def get_sequence():
    data=interface.get_sequences()
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_splitted_transcripts")
def get_splitted():
    data=interface.get_splitted_transcripts()
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_full_data/{file_id}")
def get_particular(file_id: str):
    data=interface.get_particular_data(file_id=file_id)
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_most_common")
def get_most_common():
    data=interface.get_most_common()

    data = {'data': data}
    json_data = parse_json(data)
    return json_data


def get_diarizer_server_response(self,file_path):
    # Specify the URL of the FastAPI server
    url = 'http://110.93.240.107:8880/uploadfile/'
    files = {'file': (file_path, open(file_path, 'rb'), 'audio/wav')}

    # Send a POST request to the FastAPI server with the file data
    response = requests.post(url, files=files)

    # Check the response
    if response.status_code == 200:
        print('File uploaded successfully.')
        resp_json = json.loads(response.text)
        return True,resp_json
    else:
        print('Error occurred while uploading the file:', response.text)
        return False,{}
    

def zip_extractor_1(file_names):     
    # opening the zip file in READ mode
    file_address = []
    with zipfile.ZipFile(file_names, "r") as zip_ref:
        for file_info in zip_ref.infolist():
            extracted_path = zip_ref.extract(file_info.filename)
            file_address.append(extracted_path)
    return file_address


@app.post("/uploadfiles/")
async def create_upload_files(files: List[UploadFile] = File(...)):

    for file in files:
        # Save the uploaded file
        file_path = save_uploaded_file(file)
        # call zip function here
        # it should return all file in zip and extract 
        # put the list return to interface insert db
        # or pass one y one

        new_file_list = zip_extractor_1(file_path)
        new_file_list.pop(0)
        interface.empty_db()
        for path in new_file_list:
            status,msg = interface.get_diarizer_response(path)
            print(msg)
    return {'status': status}


def save_uploaded_file(file: UploadFile) -> str:
    # Generate a unique filename to avoid conflicts
    filename = file.filename
    print(file.filename,1234)
    # Define the directory to save the uploaded files
    upload_dir = "./uploaded_files"

    # Create the directory if it doesn't exist
    os.makedirs(upload_dir, exist_ok=True)

    # Save the file in the upload directory
    file_path = os.path.join(upload_dir, filename)
    with open(file_path, "wb") as f:
        f.write(file.file.read())
    return file_path


# Routes for logs


@app.get("/get_new_data")
def get_new_data():
    data=LogInterface.get_new_data()

    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_most_phrases")
def get_most_phrases():
    data=LogInterface.get_most_phrases()

    data = {'data': data}
    json_data = parse_json(data)
    return json_data

@app.get("/get_all_logs/{state}")
def get_all_logs(state: str, start_index: int = 0, num_rows: int = 10):
    data=LogInterface.get_all_logs(state, start_index, num_rows)

    data = {'data': data}
    json_data = parse_json(data)
    return json_data

@app.get("/get_current_states/{state}")
def get_current_states(state:str,):
    # write a method for it in log interface
    data = LogInterface.get_current_states(state)
    data = {'data': data}
    json_data = parse_json(data)
    return json_data

class DispositionRequest(BaseModel):
    area_states: List[str]
    dispositions: List[str]
    area_exclude: bool
    disp_exclude: bool

@app.post("/get_dispositions")
def get_currenting_states(request: DispositionRequest):
    # write a method for it in log interface
    data = LogInterface.get_disposition_data(request.area_states, request.dispositions,
                                             request.area_exclude,request.disp_exclude)
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_area_states")
def get_currenting_1_states():
    # write a method for it in log interface
    data = LogInterface.get_area_states_data()
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_disposition_states")
def get_currenting_2_states():
    # write a method for it in log interface
    data = LogInterface.get_disposition_states_data()
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_counter_data/{state}")
def get_counter_data(state:str,):
    # write a method for it in log interface
    data = LogInterface.get_counter_data(state)
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_states")
def get_all_states():
    # write a method for it in log interface
    data = LogInterface.get_states()
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_new_states")
def get_new_states():
    # write a method for it in log interface
    data = LogInterface.get_new_states()
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_phrase_freq/{state}")
def get_phrase_freq(state: str):
    # get state from url and pass tofunction below
    data = LogInterface.get_none_responsis_pharase_freq(state = state)
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_phrase_freq")
def get_phrase_freq():
    # get state from url and pass tofunction below
    data = LogInterface.get_none_responsis_pharase_freq()
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_word_freq/{state}")
def get_word_freq(state:str):
    data=LogInterface.get_none_responis_word_freq(state=state)
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_word_freq")
def get_word_freq():
    data=LogInterface.get_none_responis_word_freq()
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_bot_hanged")
def get_bot_hanged():
    data=LogInterface.get_none_bot_hanged_up()
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_call_drop/{class_name}")
def get_states_call_drops(class_name:str):
    data=LogInterface.get_states_call_drops(class_name=class_name)
    data = {'data': data}
    json_data = parse_json(data)
    return json_data


@app.get("/get_disposition_freq")
def get_disposition_freq():
    data=LogInterface.get_disposition_freq()
    json_data = parse_json(data)
    return json_data



def zip_extractor(file_names):     
    # opening the zip file in READ mode
    file_address = []
    with zipfile.ZipFile(file_names, "r") as zip_ref:
        for file_info in zip_ref.infolist():
            extracted_path = zip_ref.extract(file_info.filename)
            file_address.append(extracted_path)
    return file_address


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    while True:
        action = await websocket.receive_text()
        if action == "get_progress":
            # You can replace this with a call to retrieve actual progress
            progress = get_current_progress()
            await websocket.send_json({"progress": progress})
        elif action == "stop":
            break

    await websocket.close()


@app.post("/uploadlogfiles/")
async def create_log_upload_files(files: List[UploadFile] = File(...),):
    global progress
    progress = 0
    for file in files:
        # Save the uploaded file
        file_path, bot_dict = save_uploaded_log_file(file)
        # call zip function here
        # it should return all file in zip and extract 
        # put the list return to interface insert db
        # or pass 
        new_file_list = zip_extractor(file_path)
        new_file_list.pop(0)
        LogInterface.empty_db()
        for path in new_file_list:
            status,msg = LogInterface.insert_to_db([path],bot_dict)
            print(msg)
            progress += 100 / len(files)
        LogInterface.new_insert_to_db()
    return {'status': 'done'}


def save_uploaded_log_file(file: UploadFile) -> str:
    filename = file.filename
    input_string = filename

    
    # Remove ".zip" extension
    input_string = input_string.replace(".zip", "")

    # Splitting the string using "|" as a separator
    result = input_string.split('|')

    # Extracting three variables
    variable1 = result[0]  # "MED"
    variable2_parts = result[1:-1]  # Exclude the first and last parts
    variable2 = " ".join(variable2_parts)  # Joining the remaining parts
    variable3 = result[-1]  # Taking the last part
    # Creating a dictionary
    bot_dict = {
        "campaign": variable1,
        "version": variable2,
        "date": variable3
    }
    upload_dir = "./uploaded_files"
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, filename)
    with open(file_path, "wb") as f:
        f.write(file.file.read())
    file_path  = os.path.abspath(file_path)
    return file_path,bot_dict


def get_current_progress():
    global progress
    return progress


progress = 0