import os
import re
import pandas as pd
from collections import Counter
import sys
import requests
import json
import re
from zipfile import ZipFile
import streamlit as st

# files_name = ["7C-D3-0A-1A-C3-C4_1676679530.txt"]
# file_names= ["zipper.zip"]
# file_name = '7C-D3-0A-1A-C3-C4_1676679530.txt'
class LogAnalytics:
    def __init__(self):
        self.item_counts = None
        self.total_calls = 0
        self.valid_calls = 0
        self.total_states = 0
        self.class_count = 0
        self.call_drop = 0
        self.count_class = 0
        self.most_common_ngrams = []
        self.content = []
        self.files = []
        self.calls = []
        self.none_calls_1 = []
        self.none_calls_2 = []
        self.none_calls_3 = []
        self.state_str = []
        self.state_seq_call = []
        self.state_seq = []
        self.trans_list = []
        self.transcripts = []
        self.state_keywords = ['ello', 'ntro', 'nterested', 'AGE', 'Age', 'ransfer', 'achine', 'reeting', 'reetings', 'itch', 'OPT', 'arrier', 'panish', 'DNC', 'dnc', 'busy', 'Busy', 'ositive', 'egative', 'XFER', 'ualifies', 'ualified']
        self.number_data = {}
        self.filings = str
        self.filers_name = []
        self.splitted_calls_3 = []
        self.path = "/home/idrak/Desktop/idrak_work/call_analytics_tool/uploaded_files/"
        self.state_dict = {
            "playing hello": "hello",
            "playing intro": "intro",
            "playing Pitch OPT": "pitch",
            "No Answer": "no_answer",
            "Hang Up": "hang_up",
            "Caller Hanged Up": "caller_hanged_up",
            "Bot Hanged Up": "bot_hanged_up",
            "DNC": "dnc",
        }
        self.df = ""
        self.data =""
        self.mergerd_dict = {}

    def zip_extractor(self,file_names):     
        # opening the zip file in READ mode
        for filing in file_names:
            with open(os.path.join(self.path,filing), 'r') as zip:
                # printing all the contents of the zip file
                zip.printdir()
            
                # extracting all the files
                # print('Extracting all the files now...')
                zip.extractall()
                # print('Done!')

    def fileReader(self,file_name):
        
        for filing in file_name:
            self.filings = filing
            try:
                with open(os.path.join(self.path,filing), "r") as file:
                    self.data = file.read()
                    # .decode("utf-8")
                    self.files.append(filing)
                    self.content.append(self.data)
            except FileNotFoundError:
                print(f"file not found: {filing}")
            except IOError:
                print(f"error occured while reading file: {filing}")


    def callSplitter(self):
        for callContent in self.content:
            if "call ended!!!" in callContent:
                splitted_calls = callContent.split("call ended!!!")
                for call in splitted_calls:
                    self.calls.append(call)


    def callCounter(self):
        self.total_calls = len(self.calls)
        return self.total_calls


    def getStates(self):
        for data in self.content:
            for line in data.splitlines():
                # check if state line not exists in state_str (prevent duplicates) then append it.
                if line not in self.state_str:
                    for kw in self.state_keywords:
                        if "----" in line and kw in line:   
                            self.state_str.append(line)


    def stateSequence(self):
        # getting all states from a call storing into list state_seq

        for call in self.calls:
            # print(call)
            state_seq_call = []
            for line in call.splitlines():
                # print(line)
                # check if iterated line exists in state's list. Then append it in sequence list
                if line in self.state_str:   
                    # print(line)     
                    state_seq_call.append(line.lower())
            # print(self.state_seq_call)
            self.state_seq.append(state_seq_call)
            # print(self.state_seq)
            # self.state_seq_call.clear()


    def countValidCalls(self):
        for call_sequence in self.state_seq:
            if len(call_sequence)>=2:
                if "achine" not in call_sequence[-1] and "DNC" not in call_sequence[-1] and "dnc" not in call_sequence[-1] and "ualified" not in call_sequence[-1]:
                        self.valid_calls+=1
            else:
                continue


    def countCallDrops(self, class_name):
        for call_sequence in self.state_seq:
            if len(call_sequence)>=2:
                if class_name in call_sequence[-1]:
                    self.call_drop+=1
            else:
                continue             


    def countClass(self,class_name):
        for call_sequence in self.state_seq:
            for state in call_sequence:
                if class_name in state:
                    self.count_class+=1


    def countMostUsedPharses(self):
        
        for call_data in self.content:
            for line in call_data.splitlines():
                if "AI bot got this data =" in line:
                    trans_line = line.split("=")[-1]
                    self.trans_list.append(trans_line)
                    self.transcripts = list(set(self.trans_list))

        # WORKING TO EXTRACT MOST USED PHRASES

        # preprocess transcripts
        self.transcripts = [re.sub(r'[^\w\s]', '', transcript.lower()) for transcript in self.transcripts]

        # count n-grams
        n = 10 # length of phrase in words
        ngrams = Counter()
        for transcript in self.transcripts:
            words = transcript.split()
            ngrams.update(' '.join(words[i:i+n]) for i in range(len(words)-n+1))

        # get most common n-grams
        self.most_common_ngrams = ngrams.most_common(5)

    def none_separator_1(self,):
        data_lines = self.data.split('\n')
        results = []
        result = {}
        # "DNC", "Not Interested", "Ans Machine", "Transfer",
        states_to_find = ["DNC", "Not Interested", "Ans Machine","Hang Up","Not Qualified","Negative","Positive", "Transfer","Bot Hanged UP","No Answer","Caller Hanged Up"]
        phone_num = '123'
        print(len(data_lines))
        for i in range(len(data_lines)):
            line = data_lines[i]

            # Detect phone number
            phone_num_match = re.search(r"Incoming: \((\d{3})\)(\d{3})-(\d{4})", line)
            if phone_num_match:
                phone_num = phone_num_match.group(0).split(':')[1].strip()
                result['Phone Number'] = phone_num_match.group(0).split(':')[1].strip()
            if '------------ playing' in line:
                line = line.replace("-","")
                line = line.replace("playing","")
                line = line.strip()
                result['Current State'] = line
            # Detect AI bot data
            ai_bot_match = re.search(r"AI bot got this data = (.*)", line)
            if ai_bot_match:
                result['AI bot got this data'] = ai_bot_match.group(1)

            # Detect AI bot level
            ai_bot_level_match = re.search(r"AI bot level= 1 : None", line)
            if ai_bot_level_match:
                result['AI bot level'] = ai_bot_level_match.group(0)
                for j in range(i+1, min(i+5, len(data_lines))):
                    next_line = data_lines[j]
                    if 'playing' in next_line:
                        continue
                j = i+1
                next_line = data_lines[min(j, len(data_lines)-1)]
                result['Next State'] = []
                token = False
                while 'call started' not in next_line:
                    for state in states_to_find:
                        if state in next_line:
                            result['Next State'] += [state]
                            break
                    if j >= len(data_lines):
                        print(True)
                        break
                    j += 1
                    next_line= data_lines[min(j, len(data_lines)-1)]
                result['Phone Number'] = phone_num
                result['Next State'] = list(set(result['Next State']))    
                results.append(result.copy())  # Save this result
                result = {}  # Clear for next potential result
        
        print(len(results))
        if len(results) > 0:

            self.df_temp = pd.DataFrame(results)
            print(self.df_temp)
            # st.dataframe(df)
            # df = df.dropna()
            # df.to_csv(f'{file_name[:-4]}_processed.csv', index=False)
        else:
            self.df_temp = pd.DataFrame()
            print(f"No results found in")


    def none_separator_2(self,):
        # Split the data by call start
        calls = self.data.split('call started')

        rows = []

        # For each call
        for call in calls[1:]:  # The first split is empty
            row = {}

            # Find phone number
            phone_num_match = re.search(r"Incoming: \((\d{3})\)(\d{3})-(\d{4})",call)
            if phone_num_match:
                phone_num = phone_num_match.group(0).split(':')[1].strip()
                row['Phone Number'] = phone_num_match.group(0).split(':')[1].strip()
            
            # States
            states = []
            for line in call.split("\n"):
                for state in self.state_dict:
                    if state in line:
                        states.append(self.state_dict[state])
            row['states'] = ', '.join(states)

            # Append to the rows
            rows.append(row)

        # Convert to DataFrame
        df = pd.DataFrame(rows)
 

        merged_df = self.df_temp.merge(df, on='Phone Number', how='inner')
        merged_dict_temp=merged_df.to_dict('records')
        self.mergerd_dict = merged_dict_temp




    def numberData(self):
        '''
        to extract number data ... 
        I made some changes and comment for that. 
        
        '''
        number_trans = []
        number_dis   = []
        numbers = []
       
        for call_index,call in enumerate(self.calls):

            number_transcript = [] #every time we make a transcript list
            if "AI bot got this data =" in call and "Incoming:" in call and "Disposition =" in call:
                splited_lines = call.splitlines()
                
                for line_index,line in enumerate(splited_lines):
                    if "Incoming:" in line:
                        number = line.split(" ")[1]
                        numbers.append(number)
                    if "AI bot got this data =" in line:
                        text = line.split("=")[1]
                        number_transcript.append(text)
                    if  "Disposition =" in line:
                        disposition = line.split("=")[1]
                        disposition = disposition[:-19] # remove the line containing slowing with 2sec etc (By. ALI)
                        number_dis.append(disposition)
                
                number_trans.append(number_transcript[0]) # getting only first Index of Transcript list

        # Display in One Dataframe
        try:
            df_number_data = {"file_id":self.filings,"Caller_ID":{self.filings:numbers}, "Transcript":{self.filings:number_trans}, "Disposition":{self.filings:number_dis}, "AI None Separater":{self.filings:self.mergerd_dict}, "total_calls":self.total_calls, "valid_calls":self.valid_calls, "total_states": self.count_class, "call_drop": self.call_drop }
            self.number_data = df_number_data       
        except:
            pass 


    def driver(self,files_name):
        class_name = ""
        # self.zip_extractor(file_names)        
        self.fileReader(files_name)
        self.callSplitter()
        self.callCounter()
        self.getStates()
        self.stateSequence()
        self.countCallDrops(class_name)
        self.countValidCalls()
        self.countClass(class_name)
        self.countMostUsedPharses()
        self.none_separator_1()
        self.none_separator_2()
        self.numberData()
        self.df_temp = pd.DataFrame()
        self.mergerd_dict = {}
        self.total_calls = 0
        self.valid_calls = 0
        self.total_states = 0
        self.class_count = 0
        self.call_drop = 0
        self.count_class = 0
        self.most_common_ngrams = []
        self.content = []
        self.files = []
        self.calls = []
        self.state_str = []
        self.state_seq_call = []
        self.state_seq = []
        self.trans_list = []
        self.transcripts = []
        self.df = ""
        self.data =""
        self.mergerd_dict = {}
        self.filings = str
        self.filers_name = []
        self.splitted_calls_3 = []

        return self.number_data
    
