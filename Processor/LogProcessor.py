import os
import re
import pandas as pd
from collections import Counter
import sys
import requests
import json
import re
from zipfile import ZipFile

class LogAnalytics:
    def __init__(self):
        # For fileReader 
        self.filings = str
        self.path = "E://Python Practice//call_analytics_tool//uploaded_files//"
        self.data = ""
        self.files = []
        self.content = []
        # For callSplitter
        self.calls = []
        # For callCounter
        self.total_calls = 0
        # For getStates
        self.state_keywords = ['ello', 'ntro', 'AGE','transferring call', 'Age', 'ransfer', 'reeting', 'reetings', 'itch', 'OPT',]
        self.state_str = []
        # For stateSequence
        self.state_seq = []
        # For new_states_counter
        self.new_dict= {}
        # For countValidCalls
        self.valid_calls = 0
        # For countCallDrops
        self.call_drop = 0
        # For countClass
        self.count_class = 0
        # For none_separator_2
        self.mergerd_dict = {}
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
        self.area_code_mapping = {
            "201": "New Jersey",
            "202": "District of Columbia",
            "203": "Connecticut",
            "205": "Alabama",
            "206": "Washington",
            "207": "Maine",
            "208": "Idaho",
            "209": "California",
            "210": "Texas",
            "212": "New York",
            "213": "California",
            "214": "Texas",
            "215": "Pennsylvania",
            "216": "Ohio",
            "217": "Illinois",
            "218": "Minnesota",
            "219": "Indiana",
            "224": "Illinois",
            "225": "Louisiana",
            "228": "Mississippi",
            "229": "Georgia",
            "231": "Michigan",
            "234": "Ohio",
            "239": "Florida",
            "240": "Maryland",
            "248": "Michigan",
            "251": "Alabama",
            "252": "North Carolina",
            "253": "Washington",
            "254": "Texas",
            "256": "Alabama",
            "260": "Indiana",
            "262": "Wisconsin",
            "267": "Pennsylvania",
            "269": "Michigan",
            "270": "Kentucky",
            "272": "Pennsylvania",
            "276": "Virginia",
            "281": "Texas",
            "301": "Maryland",
            "302": "Delaware",
            "303": "Colorado",
            "304": "West Virginia",
            "305": "Florida",
            "307": "Wyoming",
            "308": "Nebraska",
            "309": "Illinois",
            "310": "California",
            "312": "Illinois",
            "313": "Michigan",
            "314": "Missouri",
            "315": "New York",
            "316": "Kansas",
            "317": "Indiana",
            "318": "Louisiana",
            "319": "Iowa",
            "320": "Minnesota",
            "321": "Florida",
            "323": "California",
            "325": "Texas",
            "330": "Ohio",
            "331": "Illinois",
            "334": "Alabama",
            "336": "North Carolina",
            "337": "Louisiana",
            "339": "Massachusetts",
            "347": "New York",
            "351": "Massachusetts",
            "352": "Florida",
            "360": "Washington",
            "361": "Texas",
            "364": "Kentucky",
            "380": "Ohio",
            "385": "Utah",
            "386": "Florida",
            "401": "Rhode Island",
            "402": "Nebraska",
            "404": "Georgia",
            "405": "Oklahoma",
            "406": "Montana",
            "407": "Florida",
            "408": "California",
            "409": "Texas",
            "410": "Maryland",
            "412": "Pennsylvania",
            "413": "Massachusetts",
            "414": "Wisconsin",
            "415": "California",
            "417": "Missouri",
            "419": "Ohio",
            "423": "Tennessee",
            "424": "California",
            "425": "Washington",
            "430": "Texas",
            "432": "Texas",
            "434": "Virginia",
            "435": "Utah",
            "440": "Ohio",
            "442": "California",
            "443": "Maryland",
            "458": "Oregon",
            "469": "Texas",
            "470": "Georgia",
            "475": "Connecticut",
            "478": "Georgia",
            "479": "Arkansas",
            "480": "Arizona",
            "484": "Pennsylvania",
            "501": "Arkansas",
            "502": "Kentucky",
            "503": "Oregon",
            "504": "Louisiana",
            "505": "New Mexico",
            "507": "Minnesota",
            "508": "Massachusetts",
            "509": "Washington",
            "510": "California",
            "512": "Texas",
            "513": "Ohio",
            "515": "Iowa",
            "516": "New York",
            "517": "Michigan",
            "518": "New York",
            "520": "Arizona",
            "530": "California",
            "531": "Nebraska",
            "539": "Oklahoma",
            "540": "Virginia",
            "541": "Oregon",
            "551": "New Jersey",
            "559": "California",
            "561": "Florida",
            "562": "California",
            "563": "Iowa",
            "567": "Ohio",
            "570": "Pennsylvania",
            "571": "Virginia",
            "573": "Missouri",
            "574": "Indiana",
            "575": "New Mexico",
            "580": "Oklahoma",
            "585": "New York",
            "586": "Michigan",
            "601": "Mississippi",
            "602": "Arizona",
            "603": "New Hampshire",
            "605": "South Dakota",
            "606": "Kentucky",
            "607": "New York",
            "608": "Wisconsin",
            "609": "New Jersey",
            "610": "Pennsylvania",
            "612": "Minnesota",
            "614": "Ohio",
            "615": "Tennessee",
            "616": "Michigan",
            "617": "Massachusetts",
            "618": "Illinois",
            "619": "California",
            "620": "Kansas",
            "623": "Arizona",
            "626": "California",
            "630": "Illinois",
            "631": "New York",
            "636": "Missouri",
            "641": "Iowa",
            "646": "New York",
            "650": "California",
            "651": "Minnesota",
            "657": "California",
            "660": "Missouri",
            "661": "California",
            "662": "Mississippi",
            "671": "Guam",
            "678": "Georgia",
            "681": "West Virginia",
            "682": "Texas",
            "701": "North Dakota",
            "702": "Nevada",
            "703": "Virginia",
            "704": "North Carolina",
            "706": "Georgia",
            "707": "California",
            "708": "Illinois",
            "712": "Iowa",
            "713": "Texas",
            "714": "California",
            "715": "Wisconsin",
            "716": "New York",
            "717": "Pennsylvania",
            "718": "New York",
            "719": "Colorado",
            "720": "Colorado",
            "724": "Pennsylvania",
            "725": "Nevada",
            "727": "Florida",
            "731": "Tennessee",
            "732": "New Jersey",
            "734": "Michigan",
            "737": "Texas",
            "740": "Ohio",
            "747": "California",
            "754": "Florida",
            "757": "Virginia",
            "760": "California",
            "762": "Georgia",
            "763": "Minnesota",
            "765": "Indiana",
            "769": "Mississippi",
            "770": "Georgia",
            "772": "Florida",
            "773": "Illinois",
            "774": "Massachusetts",
            "775": "Nevada",
            "779": "Illinois",
            "781": "Massachusetts",
            "785": "Kansas",
            "786": "Florida",
            "801": "Utah",
            "802": "Vermont",
            "803": "South Carolina",
            "804": "Virginia",
            "805": "California",
            "806": "Texas",
            "808": "Hawaii",
            "810": "Michigan",
            "812": "Indiana",
            "813": "Florida",
            "814": "Pennsylvania",
            "815": "Illinois",
            "816": "Missouri",
            "817": "Texas",
            "818": "California",
            "828": "North Carolina",
            "830": "Texas",
            "831": "California",
            "832": "Texas",
            "843": "South Carolina",
            "845": "New York",
            "847": "Illinois",
            "848": "New Jersey",
            "850": "Florida",
            "854": "South Carolina",
            "856": "New Jersey",
            "857": "Massachusetts",
            "858": "California",
            "859": "Kentucky",
            "860": "Connecticut",
            "862": "New Jersey",
            "863": "Florida",
            "864": "South Carolina",
            "865": "Tennessee",
            "870": "Arkansas",
            "872": "Illinois",
            "878": "Pennsylvania",
            "901": "Tennessee",
            "903": "Texas",
            "904": "Florida",
            "906": "Michigan",
            "907": "Alaska",
            "908": "New Jersey",
            "909": "California",
            "910": "North Carolina",
            "912": "Georgia",
            "913": "Kansas",
            "914": "New York",
            "915": "Texas",
            "916": "California",
            "917": "New York",
            "918": "Oklahoma",
            "919": "North Carolina",
            "920": "Wisconsin",
            "925": "California",
            "928": "Arizona",
            "929": "New York",
            "931": "Tennessee",
            "936": "Texas",
            "937": "Ohio",
            "938": "Alabama",
            "940": "Texas",
            "941": "Florida",
            "947": "Michigan",
            "949": "California",
            "951": "California",
            "952": "Minnesota",
            "954": "Florida",
            "956": "Texas",
            "959": "Connecticut",
            "970": "Colorado",
            "971": "Oregon",
            "972": "Texas",
            "973": "New Jersey",
            "975": "Missouri",
            "978": "Massachusetts",
            "979": "Texas",
            "980": "North Carolina",
            "984": "North Carolina",
            "985": "Louisiana",
            "989": "Michigan",
            "1001": "Massachusetts",
            "1002": "Massachusetts",
            "1003": "Massachusetts",
            "1004": "Massachusetts",
            "1005": "Massachusetts",
            "1006": "Massachusetts",
            "1007": "Massachusetts",
            "1008": "Massachusetts",
            "1009": "Massachusetts",
            "1010": "Massachusetts",
            "1011": "Massachusetts",
            "1012": "Massachusetts",
            "1013": "Massachusetts",
            "1014": "Massachusetts",
            "1015": "Massachusetts",
            "1016": "Massachusetts",
            "1017": "Massachusetts",
            "1018": "Massachusetts",
            "1019": "Massachusetts",
            "1020": "Massachusetts",
            "1021": "Massachusetts",
            "1022": "Massachusetts",
            "1023": "Massachusetts",
            "1024": "Massachusetts",
            "1025": "Massachusetts",
            "1026": "Massachusetts",
            "1027": "Massachusetts",
            "1028": "Massachusetts",
            "1029": "Massachusetts",
            "1030": "Massachusetts",
            "1031": "Massachusetts",
            "1032": "Massachusetts",
            "1033": "Massachusetts",
            "1034": "Massachusetts",
            "1035": "Massachusetts",
            "1036": "Massachusetts",
            "1037": "Massachusetts",
            "1038": "Massachusetts",
            "1039": "Massachusetts",
            "1040": "Massachusetts",
            "1041": "Massachusetts",
            "1042": "Massachusetts",
            "1043": "Massachusetts",
            "1044": "Massachusetts",
            "1045": "Massachusetts",
            "1046": "Massachusetts",
            "1047": "Massachusetts",
            "1048": "Massachusetts",
            "1049": "Massachusetts",
            "1050": "Massachusetts",
            "1051": "Massachusetts",
            "1052": "Massachusetts",
            "1053": "Massachusetts",
            "1054": "Massachusetts",
            "1055": "Massachusetts",
            "1056": "Massachusetts",
            "1057": "Massachusetts",
            "1058": "Massachusetts",
            "1059": "Massachusetts",
            "1060": "Massachusetts",
            "1061": "Massachusetts",
            "1062": "Massachusetts",
            "1063": "Massachusetts",
            "1064": "Massachusetts",
            "1065": "Massachusetts",
            "1066": "Massachusetts",
            "1067": "Massachusetts",
            "1068": "Massachusetts",
            "1069": "Massachusetts",
            "1070": "Massachusetts",
            "1071": "Massachusetts",
            "1072": "Massachusetts",
            "1073": "Massachusetts",
            "1074": "Massachusetts",
            "1075": "Massachusetts",
            "1076": "Massachusetts",
            "1077": "Massachusetts",
            "1078": "Massachusetts",
            "1079": "Massachusetts",
            "1080": "Massachusetts",
            "1081": "Massachusetts",
            "1082": "Massachusetts",
            "1083": "Massachusetts",
            "1084": "Massachusetts",
            "1085": "Massachusetts",
            "1086": "Massachusetts",
            }
# Feel free to add more area codes and their corresponding states as needed


    
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


    def none_separator_1(self,):
        data_lines = self.data.split('\n')
        results = []
        result = {}
        # "DNC", "Not Interested", "Ans Machine", "Transfer",
        states_to_find = ["DNC", "Not Interested", "Ans Machine","Hang Up","Not Qualified","Negative","Positive", "Transfer","Bot Hanged UP","No Answer","Caller Hanged Up"]
        phone_num = '123'
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
            list_dat = ['No Answer','- DNC -','- Not Qualified -','- Not Interested -','- Ans Machine -','Transferring call']
            for dat in list_dat:
                if dat in line:
                    line = line.replace("-","")
                    line = line.strip()
                    result['Current State'] = line
            # Detect AI bot data
            ai_bot_match = re.search(r"AI bot got this data = (.*)", line)
            if ai_bot_match:
                result['AI bot got this data'] = ai_bot_match.group(1)

            # Replace AI algo_Bt_bot level = 1 with AI bot level= 1 : None for old files
            # Replace result['AI algo_Bt_bot level'] with result['AI bot level']
            # Detect AI bot level
            ai_bot_level_match_1 = re.search(r"AI algo_Bt_bot level = 1", line)
            ai_bot_level_match_2 = re.search(r"AI bot level= 1 : None", line)
            if ai_bot_level_match_1:
                result['AI bot level'] = ai_bot_level_match_1.group(0)
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
            elif ai_bot_level_match_2:
                result['AI bot level'] = ai_bot_level_match_2.group(0)
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
                
        
        if len(results) > 0:
            self.df_temp = pd.DataFrame(results)
            # st.dataframe(df)
            # df = df.dropna()
            # df.to_csv(f'{file_name[:-4]}_processed.csv', index=False)
        else:
            self.df_temp = pd.DataFrame()
            print(f"No results found in")


    def none_separator_2(self):
        # Split the data by call start
        calls = self.data.split('call started')
        rows = []

        # For each call
        for call in calls[1:]:  # The first split is empty
            row = {}

            # Find phone number
            phone_num_match = re.search(r"Incoming: \((\d{3})\)(\d{3})-(\d{4})", call)
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

        # Check if 'Phone Number' exists in both DataFrames before merging
        if 'Phone Number' in self.df_temp.columns and 'Phone Number' in df.columns:
            # Merge only if the column exists in both DataFrames
            merged_df = self.df_temp.merge(df, on='Phone Number', how='inner')
            merged_dict_temp = merged_df.to_dict('records')
            self.mergerd_dict = merged_dict_temp
        else:
            # Handle the case where 'Phone Number' is not present in one of the DataFrames
            print("Error: 'Phone Number' column not found in one of the DataFrames.")
            self.mergerd_dict = None  # or set to an appropriate default value or handle accordingly


    def numberData(self,bot_dict):
        '''
        to extract number data ... 
        I made some changes and comment for that. 
        
        '''
        number_trans = []
        number_dis   = []
        numbers = []
        dict_area_state = {}
        dict_trans = {}
        dict_dis = {}
        dict_state = {}
       
        for call_index,call in enumerate(self.calls):
            state_seq_call = []
            number_transcript = [] #every time we make a transcript list
            if "AI bot got this data =" in call and "Incoming:" in call and "Disposition =" in call:
                splited_lines = call.splitlines()
                i = 0
                for line_index,line in enumerate(splited_lines):
                    # Replace line.split(" ")[5] with line.split(" ")[1] for old files
                    if "Incoming:" in line:
                        lengthy = len(line)
                        if lengthy == 51:
                            number = line.split(" ")[1]
                            numbers.append(number)
                        elif lengthy == 67:
                            number = line.split(" ")[5]
                            numbers.append(number)
                        else:
                            number = line.split(" ")[5]
                            numbers.append(number)
                        number_news = number[1:4]
                        area_state = self.area_code_mapping.get(number_news, "UNKNOWN")
                        dict_area_state[number] = area_state

                    if "AI bot got this data =" in line:
                        text = line.split("=")[1]
                        number_transcript.append(text)
                        dict_trans[number] = text
                        
                    if  "Disposition =" in line:
                        disposition = line.split("=")[1]
                        disposition = disposition[:-19] # remove the line containing slowing with 2sec etc (By. ALI)
                        disposition = disposition.replace(" ", "")
                        number_dis.append(disposition)
                        dict_dis[number] = disposition

                    if line in self.state_str:  
                        line = line.replace("-","")
                        line = line.replace("playing","")
                        line = line.strip()  
                        state_seq_call.append(line.lower())

                dict_state[number] = state_seq_call
                self.new_dict[number] = state_seq_call 
                
                number_trans.append(number_transcript[0]) # getting only first Index of Transcript list
                # Extracting the file name without extension
                file_name_without_extension = os.path.splitext(os.path.basename(self.filings))[0]
                bot_dict["VM"] = file_name_without_extension
        # Display in One Dataframe
        try:
            df_number_data = {"file_id":self.filings,"bot_specs":bot_dict, "Caller_ID":{self.filings:numbers}, "area_state":{self.filings:dict_area_state}, "Transcript":{self.filings:dict_trans},"states_number":{self.filings:dict_state}, "Disposition":{self.filings:dict_dis}, "AI None Separater":{self.filings:self.mergerd_dict}, "total_calls":self.total_calls, "valid_calls":self.valid_calls, "total_states": self.count_class, "call_drop": self.call_drop }
            self.number_data = df_number_data       
        except:
            pass 


    def driver(self,files_name,bot_dict):
        class_name = ""      
        self.fileReader(files_name)
        self.callSplitter()
        self.callCounter()
        self.getStates()
        self.stateSequence()
        self.countCallDrops(class_name)
        self.countValidCalls()
        self.countClass(class_name)
        self.none_separator_1()
        self.none_separator_2()
        self.numberData(bot_dict)
        self.df_temp = pd.DataFrame()
        # For fileReader 
        self.filings = str
        self.data = ""
        self.files = []
        self.content = []
        # For callSplitter
        self.calls = []
        # For callCounter
        self.total_calls = 0
        # For getStates
        self.state_str = []
        # For stateSequence
        self.state_seq = []
        # For new_states_counter
        self.new_dict= {}
        # For countValidCalls
        self.valid_calls = 0
        # For countCallDrops
        self.call_drop = 0
        # For countClass
        self.count_class = 0
        # For none_separator_2
        self.mergerd_dict = {}

        return self.number_data
    
