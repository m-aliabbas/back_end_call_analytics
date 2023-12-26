from similarity import SimilarityFinder


Similarity = SimilarityFinder()
bot_sentences = Similarity.bot_sentences


   
def get_particular_data_for_segregation(self,file_id):
    data = self.DB.find({'file_id':file_id})
    splitted_transcript= data['data'][0]['spliited_trans']
    id =file_id
    transcript_list=splitted_transcript['splitted_transcript'][list(splitted_transcript['splitted_transcript'].keys())[0]]
    speakers_list=splitted_transcript['speakers'][list(splitted_transcript['speakers'].keys())[0]]
    
    bot_indexes = Similarity.similarityFinder(bot_sentences, transcript_list)
    bot_indexes = {list(set(bot_indexes))}
    
    
    for index ,speaker in enumerate(speakers_list):
        if index in bot_indexes:
            speakers_list[index] = 'Agent'
        else:
            speakers_list[index] = 'Customer'
    print(data)
    return data