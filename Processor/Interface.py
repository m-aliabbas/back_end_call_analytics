from Processor.CallProcessor import CallProcessor
import pandas as pd
from utils.utils import extract_phrases,jsonify
from DataSource.Mongo_DB import Mongo_DB
import requests
import json
import os
from collections import Counter
from Processor.similarity import SimilarityFinder
import re


Similarity = SimilarityFinder()
bot_sentences = Similarity.bot_sentences

class Interface:
    def __init__(self,keyword_file='Example_Data/insurance.json'):
        df_dict = pd.read_json(keyword_file).to_dict()
        keywords = extract_phrases(df_dict)
        self.call_processor = CallProcessor(keywords=keywords)
        # self.DB = Mongo_DB() # paramaeters are inserted in Construct
        self.DB = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                 collection_name='call_record7',)
    
    def get_diarizer_server_response(self,file_path):
        # Specify the URL of the FastAPI server
        url = 'http://192.168.100.100:8880/uploadfile/'
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

    def get_diarizer_response(self,file_path=''):
        status=False
        msg='Went Wrong'
        # diarizer_response = self.get_diarizer_server_response()
        # print(file_path)
        status, msg = self.get_diarizer_server_response(file_path)
        print(status,msg)

        msg1 = msg['msg']
        msg1['file_id'] = os.path.basename(msg1['file_id'])
        splitted_trans,full_transcript,sequence_dict = self.call_processor.process_input(input_dict=msg1)
        
        file_id = list(full_transcript.keys())[0]
        print(file_id)
        # splitted_trans[file_id]
        data_to_save = {'file_id': file_id, 'spliited_trans':splitted_trans,'full_transcript':full_transcript,'sequence_dict':sequence_dict}
        print(data_to_save)
        status, msg = self.insert_to_db(data_to_save)
        return status,msg
        
    

    def insert_to_db(self,data):
        file_id = data['file_id']
        if  self.DB.check_if_exists(file_id=file_id):
            return True, 'Data  already exists'
        else:
            temp_=self.DB.insert(data=data)
            if temp_:
                return True, 'Data Added successfully'
            else:
                return False,'Something went wrong'
            
    def empty_db(self,):
        temp = self.DB.empty()
        if temp:
            print('Eliminated')
            return True, 'Data Eliminated successfully'
        else:
            print('Error')
            return False,'Something went wrong'

    def get_complete_data(self):
        data = self.DB.find()
        return data
        
    
    def get_full_transripts(self):
        data = self.DB.find({},['full_transcript','file_id'])
        return data

    def get_splitted_transcripts(self):
        data = self.DB.find({},['spliited_trans','file_id'])
        return data

    def get_sequences(self):
        data = self.DB.find({},['sequence_dict','file_id'])
        return data

    # def get_particular_data(self,file_id):
    #     data = self.DB.find({'file_id':file_id})
    #     print(data)
    #     return data
    
    # def get_particular_data(self,file_id):
    #     data = self.DB.find({'file_id':file_id})
    #     # print(data)
    #     splitted_transcript= data[0]['spliited_trans']
    #     id =file_id
    #     transcript_list=splitted_transcript['splitted_transcript'][list(splitted_transcript['splitted_transcript'].keys())[0]]
    #     speakers_list=splitted_transcript['speakers'][list(splitted_transcript['speakers'].keys())[0]]
    #     bot_keywords = ["Senior Citizens Care","Senior Benefits","US Auto care","Auto care","Home Improvement Services","American Solar","Medicare department","health care benefits","Auto warrant processing center","local energy advisers","American senior citizen care"]
    #     index1 = 0 
    #     for key_word in bot_keywords:
    #         for index,transcript in enumerate(transcript_list):
    #             if key_word in transcript:
    #                 index1=index
    #     speaker_name = speakers_list[index1]
    #     for i,speaker in enumerate(speakers_list):
    #         print(1,speaker_name,speaker)
    #         if speaker_name == speaker:
    #             speakers_list[i] = 'Agent'
    #         else:
    #             speakers_list[i] = 'Customer'
    #     # print(data)
    #     return data
    


    def get_particular_data(self,file_id):
        data = self.DB.find({'file_id':file_id})
        splitted_transcript= data[0]['spliited_trans']
        id =file_id
        transcript_list=splitted_transcript['splitted_transcript'][list(splitted_transcript['splitted_transcript'].keys())[0]]
        speakers_list=splitted_transcript['speakers'][list(splitted_transcript['speakers'].keys())[0]]
        
        bot_indexes = Similarity.similarityFinder(bot_sentences, transcript_list)
        bot_indexes = list(set(bot_indexes))
        
        
        for index ,speaker in enumerate(speakers_list):
            if index in bot_indexes:
                speakers_list[index] = 'Agent'
            else:
                speakers_list[index] = 'Customer'
        print(data)
        return data
    


    def get_most_common(self):
        data = self.DB.find({},['spliited_trans'])

        try:
            # Initialize an empty list to store all cleaned phrases
            all_phrases = []

            # Loop through each entry in the data list to collect all phrases
            for entry in data:
                for key in entry['spliited_trans']['splitted_transcript']:
                    phrases = entry['spliited_trans']['splitted_transcript'][key]
                    for phrase in phrases:
                        all_phrases.append(phrase)

            # Clean up each phrase by removing special characters and Filter out phrases with less than 4 words
            cleaned_phrases = []
            for phrase in all_phrases:
                if len(phrase.split()) >= 4:
                    clean_phrase = re.sub(r'[^a-zA-Z0-9\s]', '', phrase)
                    cleaned_phrases.append(clean_phrase)

            # Count occurrences of each phrase
            phrase_counts = Counter(cleaned_phrases)

            # Sort phrases by their counts
            sorted_phrases = sorted(phrase_counts.items(), key=lambda x: x[1], reverse=True)

            # Extract only the top 5 phrases
            result = dict(sorted_phrases[:5])

            data_response = {"status": True, "data": result, "msg": "data got"}

        except Exception as e:
            print(e)
            data_response = {"status":False,"data":{},"msg":f"You got the error {e}"}

        return data_response
        

if __name__ == "__main__":
    interface = Interface()
    msg,resp=interface.get_diarizer_response(diarizer_response={
        'file_id': '20220328-102401_6623727904-all231',
        'SPEAKER_01_1': {'trascript': ' Hello? Hello? Hi, this is Amy with American Senior Citizens Care. How are you doing today?'},
        'SPEAKER_00_1': {'trascript': " I'm five."},
        'SPEAKER_01_2': {'trascript': ' Sorry to hear that. This call is about a new state regulated final expense.'},
        'SPEAKER_00_2': {'trascript': ' Insurance Plan which covers 100% of your burial funerals.'},
        'SPEAKER_01_3': {'trascript': ' or cremation expenses. It is specifically designed for the people on fixed income or social security. Would you like to learn more about...'},
        'SPEAKER_00_3': {'trascript': ' Yes.'},
        'SPEAKER_01_4': {'trascript': ' qualify you for the plan, are you between the age of 40 and 80? Yes. Are you capable enough to make your own financial decisions?'},
        'SPEAKER_00_4': {'trascript': ' Yeah.'},
        'SPEAKER_01_5': {'trascript': ' Great! Let me bring the state licensed product specialist on the line to share information with you. Please hold.'}
        })
    print(msg,resp)
    