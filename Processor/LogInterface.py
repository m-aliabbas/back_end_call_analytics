from Processor.LogProcessor import LogAnalytics
from DataSource.Mongo_DB import Mongo_DB
import requests
import json
import os
import re
import pandas as pd
from collections import Counter
import itertools

def cleanify(text):
    # Convert the text to lowercase
    lower_text = text.lower()

    # Remove special characters using regular expression
    cleaned_text = re.sub(r'[^a-zA-Z0-9\s]', '', lower_text)

    return cleaned_text

def split_string_into_words(input_string):
    return input_string.strip().split()

class LogInterface:
    def __init__(self,):
        self.log_processor = LogAnalytics()
        self.DB_1 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='log_record',)
        
        self.DB_2 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='new_log_record',)
        
        self.DB_3 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='get_states_record',)
        
        self.DB_4 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='get_new_states_record',)
        
        self.DB_5 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='merged_record',)
        
        self.DB_6 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='all_record',)
        
        self.DB_7 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='age-question_record',)
        

        self.DB_8 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='greetings_record',)
        

        self.DB_9 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='hanging-up-with-transfer_record',)
        

        self.DB_10 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='hello_record',)
        
        self.DB_11 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='intro_record',)
        
        self.DB_12 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='no-pitch-price_record',)
        
        self.DB_13 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='pitch_record',)
        
        self.DB_14 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='pitch-opt_record',)
        
        self.DB_15 = Mongo_DB(address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                   collection_name='transfer_record',)
    
    def insert_to_db(self,file_name,bot_dict):
        print("files_name: ",file_name)
        data = self.log_processor.driver(files_name=file_name,bot_dict = bot_dict)
        file_id = data["file_id"]
        
        if self.DB_1.check_if_exists(file_id=file_id):
            print('Already Exists')
            return True, 'Data  already exists'
        else:
            temp_=self.DB_1.insert(data=data)
            if self.DB_2.check_if_exists(file_id=file_id):
                print('Already Exists')
            else:
                temp_=self.DB_2.insert(data=data)
                print('Inserted in DB_2')
            if temp_:
                print('Inserted in DB_1')
                return True, 'Data Added successfully'
            else:
                print('Error')
                return False,'Something went wrong' 
            

    def new_insert_to_db(self,):
        # Fetching data from the database
        data_lists = self.DB_1.find()
        
        # Initializing counters and lists
        total_calls = 0
        valid_calls = 0
        call_drop = 0
        Caller_ID_List = []
        Transcript_List = []
        Disposition_List = [] 
        File_ID_List = []
        states_number = []
        area_states = []
        bot_specs={}
        filer_id = "" 
        # Iterate through a list of dictionaries called data_lists
        for data_list in data_lists:

            # Get the 'file_id' value from the current dictionary; default to None if not found
            file_id = data_list.get('file_id', None)
            filer_id = file_id
            # If 'file_id' is None, skip this iteration
            if not file_id:
                continue
            else:
                # Extract data from the dictionary and append it to respective lists
                # Use the 'get' method to safely access dictionary keys and provide default values if they don't exist
                states_number.append(data_list.get('states_number', {}).get(file_id, {}))
                area_states.append(data_list.get('area_state', {}).get(file_id, {}))
                Transcript_List.append(data_list.get('Transcript', {}).get(file_id, {}))
                Disposition_List.append(data_list.get('Disposition', {}).get(file_id, {}))
                Caller_ID_List.append(data_list.get('Caller_ID', {}).get(file_id, {}))
                bot_specs[file_id] = data_list.get('bot_specs', {})
                # Increment total_calls and valid_calls with values from the dictionary, default to 0 if not found
                total_calls += data_list.get('total_calls', 0)
                valid_calls += data_list.get('valid_calls', 0)

                # Generate a list of File IDs and add it to File_ID_List
                # The list is created by repeating the base file ID without the extension for the length of the Caller_ID list
                File_ID_List += [os.path.basename(file_id)[:-4]] * len(data_list.get('Caller_ID', {}).get(file_id, []))

        # Initialize empty dictionaries to hold merged key-value pairs from respective lists.
        merged_dict_1 = {}
        merged_dict_2 = {}
        merged_dict_3 = {}
        merged_dict_4 = {}

        # Iterate through lists of dictionaries and update merged dictionaries.
        for d in states_number:
            merged_dict_1.update(d)

        for d in Transcript_List:
            merged_dict_2.update(d)

        for d in Disposition_List:
            merged_dict_3.update(d)

        for d in area_states:
            merged_dict_4.update(d)
        # Initialize empty lists to store new data after filtering based on Caller_ID numbers.
        States_new = []
        trans_new = []
        dispos_new = []
        number_new = []
        area_new = []

        # Iterate through Caller_ID numbers from the caller id list
        for list in Caller_ID_List:
            # Iterate through all the Caller_ID numbers
            for number in list:    
                # Retrieve corresponding data from merged dictionaries based on Caller_ID number.
                stating = merged_dict_1.get(number, None)
                trans = merged_dict_2.get(number, None)
                dispos = merged_dict_3.get(number, None)
                area = merged_dict_4.get(number, None)

                # Check if any of the data is missing (None), and if so, skip this iteration.
                if stating is None or trans is None or dispos is None:
                    continue
                else:
                    # If all data is available, add the Caller_ID number and associated data to the respective lists.
                    number_new.append(number)
                    States_new.append(stating)
                    trans_new.append(trans)
                    dispos_new.append(dispos)
                    area_new.append(area)

            # After the loop, States_new, trans_new, dispos_new, and number_new contain filtered and matched data.

        last_values = []

        for sublist in States_new:
            if sublist:  # Check if the sublist is not empty
                last_values.append(sublist[-1])
        count = Counter(last_values)

        current_state=[]
        for stater in States_new:
            modified_string = stater[-1 ].replace(" ", "-")
            current_state.append(modified_string)


        # Ensuring all lists are of the same length
        min_number = min(len(number_new), len(dispos_new), len(trans_new), len(File_ID_List), len(States_new),len(current_state),len(area_new))

        # Constructing the final data structure to be returned
        complete_data = {
            'files_id':filer_id,
            'total_calls': total_calls,
            'valid_calls': valid_calls,
            'call_drop': call_drop,
            'bot_specs':bot_specs,
            'disposition_table': {
                'caller_id': number_new,
                'area_state':area_new,
                'current_state':current_state,
                'transcript': trans_new,
                'disposition': dispos_new,
                'file_id': File_ID_List[:min_number],
                'states': States_new
            }
        }
        bot_specs=[]
        # temp_5 = self.DB_2.empty()
        # temp_5 = self.DB_3.empty()
        # temp_5 = self.DB_4.empty()
        # temp_5 = self.DB_5.empty()
        # temp_5 = self.DB_6.empty()
        # temp_5 = self.DB_7.empty()
        # temp_5 = self.DB_8.empty()
        # temp_5 = self.DB_9.empty()
        # temp_5 = self.DB_10.empty()
        # temp_5 = self.DB_11.empty()
        # temp_5 = self.DB_12.empty()
        # temp_5 = self.DB_13.empty()
        # temp_5 = self.DB_14.empty()
        
        self.final_insert_to_db(complete_data)


    def final_insert_to_db(self,data):
        new_dict={}
        alpha = data["disposition_table"]["caller_id"]
        
        i = 0

        for file in alpha:
            if self.DB_5.check_if_exists_logs(file=file):
                print('Already exists caller id: ',file)
            else:
                new_dict["caller_id"] = file
                new_dict["transcript"] = data["disposition_table"]["transcript"][i]            
                new_dict["disposition"] = data["disposition_table"]["disposition"][i] 
                new_dict["file_id"] = data["disposition_table"]["file_id"][i]
                new_dict["current_state"] = data["disposition_table"]["current_state"][i]
                new_dict["states"] = data["disposition_table"]["states"][i]
                new_dict["area_state"] = data["disposition_table"]["area_state"][i]
                new_dict["bot_specs"] = data["bot_specs"]
                temp_13 = self.DB_5.insert(data=new_dict)
                if temp_13:
                    print('Inserted merged')
                else:
                    print('Error merged')
            
            i = i + 1
            new_dict = {}

        self.insert_states()
        self.insert_new_states()
        data = self.DB_4.find()
        last_data = data[-1]
        # Create a mapping of collection names to Mongo_DB objects
        db_mapping = {
            "all": self.DB_6,
            "age-question": self.DB_7,
            "greetings": self.DB_8,
            "hanging-up-with-transfer": self.DB_9,
            "hello": self.DB_10,
            "intro": self.DB_11,
            "no-pitch-price": self.DB_12,
            "pitch": self.DB_13,
            "pitch-opt": self.DB_14,
            "transfer": self.DB_15,
        }
        # Iterate through the data
        for dat in last_data['states']:
            newly_dict = {}
            # Check if the dat is in the mapping
            if dat in db_mapping:
                # Get the corresponding Mongo_DB object
                current_db = db_mapping[dat]

                # Use the count_all_states method if dat is 'all', else use count_states
                if dat == 'all':
                    merged_dict_1 = self.DB_5.count_all_states()
                    newly_dict['total_calls'] = merged_dict_1
                    newly_dict['valid_calls'] = merged_dict_1
                    disposition = self.DB_5.find_disposition(dat)
                    newly_dict['dispostion'] = self.count_words(disposition)
                    temp_13 = current_db.insert(data=newly_dict)
                    if temp_13:
                        print(f'Inserted {dat}')
                    else:
                        print(f'Error {dat}')
                else:
                    merged_dict_1 = self.DB_5.count_all_states()
                    merged_dict_2 = self.DB_5.count_states(dat)
                    newly_dict['total_calls'] = merged_dict_1
                    newly_dict['valid_calls'] = merged_dict_2
                    disposition = self.DB_5.find_disposition(dat)
                    newly_dict['dispostion'] = self.count_words(disposition)
                    temp_13 = current_db.insert(data=newly_dict)
                    if temp_13:
                        print(f'Inserted {dat}')
                    else:
                        print(f'Error {dat}')

                # Now you can use the merged_dict as needed
                # Example: print(merged_dict)
            else:
                print(f"No Mongo_DB object found for data: {dat}")


    def insert_states(self):
        data = self.DB_1.find({}, ['AI None Separater', 'file_id'])

        if data:
            key = list(data[0]['AI None Separater'].keys())[0]
            df_temp = pd.DataFrame(data[0]['AI None Separater'][key])

            df_list = []
            if len(data) > 1:
                for i in range(1, len(data)):
                    key = list(data[i]['AI None Separater'].keys())[0]
                    df_temp1 = pd.DataFrame(data[i]['AI None Separater'][key])
                    df_list.append(df_temp1)

                if df_list:
                    df_concat = pd.concat(df_list, ignore_index=True)
                    df_temp = pd.concat([df_temp, df_concat], ignore_index=True)

                    # Resetting the index to convert it to a regular column
                    df_temp_reset = df_temp.reset_index(drop=True)

                    new_list = df_temp_reset['Current State'].value_counts().keys()
                    new_list = new_list.insert(0, 'all')
                    new_list = list(new_list)
                    stated = {}
                    # modified_list = [item.replace(' ', '-') for item in new_list]
                    stated["states"] = new_list

                    if self.DB_3.check_if_exists_states(stated=stated):
                        print('Already exists states: ',stated)
                    else:
                        temp_10 = self.DB_3.insert(data=stated)
                        if temp_10:
                            print('Inserted stated')
                        else:
                            print('Error stated')
                else:
                    # Handle the case when df_list is empty
                    print('No DataFrames in df_list')
            else:
                # Handle the case when len(data) is 1
                print('Only one DataFrame in data')
        else:
            print('No data found in DB_1')


    def insert_new_states(self):
        data = self.DB_1.find({},)
        data_lists = data
        states_number = []
        unique_list = []
        for data_list in data_lists:
            file_id = data_list['file_id']
            states_number.append(data_list['states_number'][file_id])
        # data_response = {"status":True,"data":new_list,"msg":"data got"}

        for state in states_number:
            # Using a set to store unique values
            unique_values = set()

            for key, value_list in state.items():
                for item in value_list:
                    unique_values.add(item)

            # Convert set to list
            unique_list = list(unique_values)

            unique_list = sorted(unique_list)
            # Insert a value at the beginning of the list
            value_to_insert_first = 'all'
            unique_list.insert(0, value_to_insert_first)

        stated = {}
        modified_list = [item.replace(' ', '-') for item in unique_list]
        stated["states"] = modified_list
        if self.DB_4.check_if_exists_states(stated=stated):
            print('Already exists states: ',stated)
        else:
            temp_11 = self.DB_4.insert(data=stated)
            if temp_11:
                print('Inserted stated')
            else:
                print('Error stated')    


    def empty_db(self,):
        temp = self.DB_1.empty()
        if temp:
            print('Eliminated')
            return True, 'Data Eliminated successfully'
        else:
            print('Error')
            return False,'Something went wrong'
 

    def get_states(self,):
        data = self.DB_3.find()
        try:
            last_data = data[-1]
            data_response = {"status": True, "data": last_data["states"], "msg": "data got"}
        except Exception as e:
            print(e)
            data_response = {"status":False,"data":{},"msg":f"You got the error {e}"}

        return data_response
    

    def get_new_states(self,):
        data = self.DB_4.find()
        try:
            last_data = data[-1]
            data_response = {"status": True, "data": last_data["states"], "msg": "data got"}
        except Exception as e:
            print(e)
            data_response = {"status":False,"data":{},"msg":f"You got the error {e}"}

        return data_response


    def get_current_states(self, state):
        # Converting the state to lowercase
        state = state.lower()
        
        # Initialize empty lists to store new data after filtering based on Caller_ID numbers.
        merged_dict = self.DB_5.count_states(state)

        return merged_dict
        

    def get_all_logs(self, state, start_index, num_rows):
        # Converting the state to lowercase
        state = state.lower()
        
        # Initialize empty lists to store new data after filtering based on Caller_ID numbers.
        merged_dict = self.DB_5.find_logs_range(state,start_index, num_rows)
        number_new = []
        trans_new = []
        dispos_new = []
        File_ID_List = []
        States_new = []

        for listing in merged_dict:
            number_new.append(listing["caller_id"])
            trans_new.append(listing["transcript"])
            dispos_new.append(listing["disposition"])
            File_ID_List.append(listing["file_id"])
            States_new.append(listing["states"])

        complete_data = {
            'disposition_table': {
                'caller_id': number_new,
                'transcript': trans_new,
                'disposition': dispos_new,
                'file_id': File_ID_List,
                'states': States_new
            }
        }
        df = pd.DataFrame(complete_data['disposition_table'])

        if state != 'all':
            # Select rows where the last value of the list in 'states' column is given state
            selected_rows = df[df['states'].apply(lambda x: x[-1] if x else None) == state]
            # Convert the selected rows to dictionary
            dict_representation = selected_rows.to_dict()
            complete_data['disposition_table'] = dict_representation
            complete_data['disposition_table']['caller_id'] = complete_data['disposition_table']['caller_id'].values()
            complete_data['disposition_table']['transcript'] = complete_data['disposition_table']['transcript'] .values()
            complete_data['disposition_table']['disposition'] = complete_data['disposition_table']['disposition'] .values()
            complete_data['disposition_table']['file_id'] = complete_data['disposition_table']['file_id'] .values()
            complete_data['disposition_table']['states'] = complete_data['disposition_table']['states'] .values()

        
        data_response = {"status": True, "data": complete_data, "msg": "data got"}
        return data_response

        
    def get_counter_data(self,state):
        db_mapping = {
            "all": self.DB_6,
            "age-question": self.DB_7,
            "greetings": self.DB_8,
            "hanging-up-with-transfer": self.DB_9,
            "hello": self.DB_10,
            "intro": self.DB_11,
            "no-pitch-price": self.DB_12,
            "pitch": self.DB_13,
            "pitch-opt": self.DB_14,
            "transfer": self.DB_15,
        }
        default_db = self.DB_6
    
        # Check if the state is in db_mapping, if not, use the default database
        selected_db = db_mapping.get(state, default_db)
        
        # Query the selected database and retrieve all records
        data = selected_db.find()
        
        data_response = {"status": True, "data": data, "msg": "data got"}
        return data_response
    

    def get_new_data(self,):
        # Query the database and retrieve all records
        data = self.DB_5.find()
        return data


    def get_disposition_data(self,area_states, dispositions,area_exclude,disp_exclude):
        # Query the database and retrieve all records
        columns_to_retrieve = ["caller_id", "disposition"]
        print(disp_exclude,area_exclude)
        data = self.DB_5.find_new_disposition(area_states, dispositions,
                                              area_exclude=area_exclude,disp_exclude=disp_exclude,
                                               cols=columns_to_retrieve)
        data_response = {"status": True, "data": data, "msg": "data got"}
        return data_response
    
    def get_disposition_states_data(self,):
        # Query the database and retrieve all records
        columns_to_retrieve = ["disposition"]
        data = self.DB_5.find_new_disposition_area(cols=columns_to_retrieve)
        unique_states = set(item["disposition"] for item in data)
        unique_states_list = list(unique_states)
        data_response = {"status": True, "data": unique_states_list, "msg": "data got"}
        return data_response

    def get_area_states_data(self,):
        # Query the database and retrieve all records
        columns_to_retrieve = ["area_state"]
        data = self.DB_5.find_new_disposition_area(cols=columns_to_retrieve)
        unique_states = set(item["area_state"] for item in data)
        unique_states_list = list(unique_states)
        data_response = {"status": True, "data": unique_states_list, "msg": "data got"}
        return data_response
    

    def count_words(self,word_list):
        return dict(Counter(word_list))
        

    def get_particular_data(self,file_id):
        data = self.DB_1.find({'file_id':file_id})
        return data
    
    
    def get_most_phrases(self,):
        data = self.DB_1.find({},)
        try:
            # Extract and flatten all 'Transcript' values
            all_phrases = [phrase for record in [entry['Transcript'] for entry in data] for value in record.values() for phrase in value]

            # Filter phrases longer than 3 words
            filtered_phrases = [phrase for phrase in all_phrases if len(phrase.split()) >= 4]

            # Count occurrences of each phrase
            phrase_counts = Counter(filtered_phrases)

            # Sort phrases by their counts
            sorted_phrases = sorted(phrase_counts.items(), key=lambda x: x[1], reverse=True)

            # Extract only the top 5 phrases
            result = dict(sorted_phrases[:5])

            data_response = {"status": True, "data": result, "msg": "data got"}

        except Exception as e:
            print(e)
            data_response = {"status":False,"data":{},"msg":f"You got the error {e}"}

        return data_response
    
    
    def get_disposition_freq(self,):
        data = self.DB_1.find({},)
        try:
            result = []

            for record in data:
                for value in record['Disposition'].values():
                    for phrase in value:
                        result.append(phrase)
            # Sort phrases by their counts
            word_counts = self.count_words(result)

            # Return the result
            data_response = {"status": True, "data": word_counts, "msg": "data got"}

        except Exception as e:
            print(e)
            data_response = {"status":False,"data":{},"msg":f"You got the error {e}"}

        return data_response
    

    def word_counts(self,text):
        return len(text.split(' '))


    def get_states_call_drops(self,class_name):
        try:
            class_name = class_name.lower()
            data = self.DB_1.find()

            data_lists = data
            states_number = []
            total_calls = 0
            valid_calls = 0
            dicts = {}
            for data_list in data_lists:
                file_id = data_list['file_id']
                total_calls += data_list['total_calls']
                valid_calls += data_list['valid_calls']
                states_number.append(data_list['states_number'][file_id])

            # asssigning values to dict
            dicts['total_calls'] = total_calls
            dicts['valid_calls'] = valid_calls

            last_items = []
            
            for item in states_number:
                for values in item.values():
                    if values:  # Checks if the list is not empty
                        last_items.append(values[-1])

            count = Counter(last_items)

            if class_name == 'all':
                dicts['call_drops'] = sum(count.values())
            else:
                dicts['call_drops'] = count[class_name]
            
            data_response = {"status":True,"data":dicts,"msg":"data got"}

        except Exception as e:
            data_response = {"status":True,"data":{},"msg":f"You got the error {e}"}

        return data_response
 

    def get_none_responsis_pharase_freq(self,direct_flag = False, state = 'all'):
        data = self.DB_2.find({},['AI None Separater','file_id'])
        try:
            key = list(data[0]['AI None Separater'].keys())[0]
            df_temp = pd.DataFrame(data[0]['AI None Separater'][key])

            df_list = []
            if len(data)>1:
                for i in range(1,len(data)):
                    key = list(data[i]['AI None Separater'].keys())[0]
                    df_temp1 = pd.DataFrame(data[i]['AI None Separater'][key])
                    df_list.append(df_temp1)
                df_concat = pd.concat(df_list)
                df_temp = pd.concat([df_temp,df_concat])
            df_temp = df_temp.drop_duplicates(subset='Phone Number')
            df_temp['length'] = df_temp['AI bot got this data'].apply(self.word_counts)
            if not direct_flag:
                df_temp= df_temp[df_temp['length']>=3]
            if state != 'all':
                counting = []
                for index, row in df_temp.iterrows():         
                    if row['Current State'] == state: 
                        counting.append(row['AI bot got this data'])
                        # current state jab state k equal ho to wo row nikalo
                counters = Counter(counting)
            else:    
                counting =    df_temp['AI bot got this data'].apply(cleanify).value_counts()
                counters = counting.to_dict()

            sorted_frequency_dict = {k: v for k, v in sorted(counters.items(), key=lambda item: item[1], reverse=True)}
            data_response = {"status":True,"data":sorted_frequency_dict,"msg":"data got"}
            
        except Exception as e:
            print(e)
            data_response = {"status":False,"data":{},"msg":f"You got the error {e}"}
        return data_response


    def get_none_responis_word_freq(self,state = 'all',direct_flag=True):
        data = self.get_none_responsis_pharase_freq(direct_flag=direct_flag,state=state)
        if data['status']:
            data = data['data']
            data = list(data.keys())
            list_of_words = [word for string in data for word in split_string_into_words(string)]
            frequency_dict = {}
            for item in list_of_words:
                frequency_dict[item] = frequency_dict.get(item, 0) + 1

            sorted_frequency_dict = {k: v for k, v in sorted(frequency_dict.items(), key=lambda item: item[1], reverse=True)}
            data_response = {"status":True,"data":sorted_frequency_dict,"msg":"data got"}
        else:
            data_response = {"status":False,"data":{},"msg":f"You got the error "}
        return data_response


    def get_none_bot_hanged_up(self):
        data = self.DB_2.find({},['AI None Separater','file_id'])
        try:
            key = list(data[0]['AI None Separater'].keys())[0]
            df_temp = pd.DataFrame(data[0]['AI None Separater'][key])
            # print(df_temp)
            df_list = []
            if len(data)>1:
                for i in range(1,len(data)):
                    key = list(data[i]['AI None Separater'].keys())[0]
                    df_temp1 = pd.DataFrame(data[i]['AI None Separater'][key])
                    df_list.append(df_temp1)
            df_concat = pd.concat(df_list)
            df_temp = pd.concat([df_temp,df_concat])
            # please make changes for bot hangedup
            filtered_rows = df_temp[df_temp['Next State'].apply(lambda states: 'Bot Hanged UP' in states)]
            # print(filtered_rows)
            filtered_rows.to_dict('records')
            row_data = filtered_rows.to_dict('records')
            data_response = {"status":True,"data":row_data,"msg":"data got"}
            # print(data_response)
        except Exception as e:
            print(e)
            data_response = {"status":False,"data":[],"msg":f"You got the error {e}"}
        # print(data_response)
        return data_response

