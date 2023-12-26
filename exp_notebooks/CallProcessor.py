from collections import Counter
import pandas as pd
from utils import extract_phrases
'''
Get the diarization Response;
Process It. 
'''

class CallProcessor():
    """
        CallProcessor processes the diarization response and analyzes the resulting data. 
        It has several methods for splitting the transcript by speakers, 
        generating a full transcript, and finding sequences of keywords in the transcript.
    """
    def __init__(self,keywords = {}):
        """
        Initialize CallProcessor with an optional keywords dictionary.
        """

        
        self.keywords = keywords
        self.dict_list = []
        self.splitted_trans = {}
        self.splitted_df = {}
        self.full_transcript = {}
        self.sequence_dict = {}

    def process_input(self,input_dict,keywords=None) -> tuple:
        """
        Input dict from diarizaer response
        Processes the input dictionary by extracting the transcript and driving the analysis.

        args:
            input(dict) : {
                        'file_id': '20220328-102401_6623727904-all',
                        'SPEAKER_01_1': {'trascript': ' Hello? How are you? '},
                        'SPEAKER_00_1': {'trascript': " I'm fine."}
                        } 
            returns: 
                splitted_trans(dict)
                full_transcript(dict)
                sequence_dict(dict)

        """
        if keywords is not None:
            self.keywords = keywords
        file_name = input_dict['file_id']
        whole_data = input_dict
        self.dict_list = [whole_data]
        splitted_trans,full_transcript,sequence_dict = self.driver()
        return splitted_trans,full_transcript,sequence_dict
        
        
    def get_splitted_transcript(self):  
        # Splitting the data into separate dataframes for each speaker
        try:
            for dict in self.dict_list:
                local_trans = []
                local_speaker = []
                for key, value in dict.items():
                    if "file_id" in key:
                        file_id = value
                    elif "SPEAKER" in key:
                        split_trans = value["trascript"]
                        local_trans.append(value["trascript"])
                        local_speaker.append(key.split("_")[1])
                self.splitted_trans[file_id] = local_trans
                local_speakers_dict1={file_id:local_speaker}
                df = pd.DataFrame({"speaker":local_speaker, "transcript":local_trans})
                self.splitted_df[file_id] = df

            return {'splitted_transcript':self.splitted_trans,'speakers':local_speakers_dict1}
        except Exception as e:
            print(e)
            return {'splitted_transcript':{'':[]},'speakers':{'':[]}}
            
    def get_full_transcript(self):
        # Creating a full transcript by joining the transcriptions from all speakers
        try:
            for id, trans in self.splitted_trans.items():
                new_trans = ' '.join(trans)
                self.full_transcript[id] =  new_trans
            return self.full_transcript
        except Exception as e:
            print(e)
            return {}
            
    

    def get_call_sequence(self):
        try:
            # Finding sequences of keywords in the transcriptions
            for key, value in self.splitted_trans.items():
                sequence_list = []  
                lowercase_list = [item.lower() for item in value]
                
                for k, val in self.keywords.items():
                    for each_trans in lowercase_list:       
                        if val in each_trans:
                            sequence_list.append(k.lower())                   
                        else:
                            continue

                self.sequence_dict[key] = (", ".join(sequence_list))
            return self.sequence_dict
        except Exception as e:
            print(e)
            return {}
            
    
    def driver(self,):
        # Driver function to execute the analysis steps
        
        self.splitted_trans = {}
        self.splitted_df = {}
        self.full_transcript = {}
        self.sequence_dict = {}
        splitted_trans=self.get_splitted_transcript()
        full_transcript=self.get_full_transcript()
        sequence_dict = self.get_call_sequence()
        self.dict_list = []
        return splitted_trans,full_transcript,sequence_dict
    



if __name__ == "__main__":
    df_dict = pd.read_json('Example_Data/insurance.json').to_dict()
    keywords = extract_phrases(df_dict)
    
    my_call_processor = CallProcessor(keywords=keywords)
    test_diarizer_response = {
        'file_id': '20220328-102401_6623727904-all',
        'SPEAKER_01_1': {'trascript': ' Hello? Hello? Hi, this is Amy with American Senior Citizens Care. How are you doing today?'},
        'SPEAKER_00_1': {'trascript': " I'm five."},
        'SPEAKER_01_2': {'trascript': ' Sorry to hear that. This call is about a new state regulated final expense.'},
        'SPEAKER_00_2': {'trascript': ' Insurance Plan which covers 100% of your burial funerals.'},
        'SPEAKER_01_3': {'trascript': ' or cremation expenses. It is specifically designed for the people on fixed income or social security. Would you like to learn more about...'},
        'SPEAKER_00_3': {'trascript': ' Yes.'},
        'SPEAKER_01_4': {'trascript': ' qualify you for the plan, are you between the age of 40 and 80? Yes. Are you capable enough to make your own financial decisions?'},
        'SPEAKER_00_4': {'trascript': ' Yeah.'},
        'SPEAKER_01_5': {'trascript': ' Great! Let me bring the state licensed product specialist on the line to share information with you. Please hold.'}
        } 
    splitted_trans,full_transcript,sequence_dict = my_call_processor.process_input(test_diarizer_response)
    print('\n-----Spliited Trans-----\n',splitted_trans,
          '\n-----Full Trans-----\n',full_transcript,
          '\n-----Sequence Dict-----\n',sequence_dict,)