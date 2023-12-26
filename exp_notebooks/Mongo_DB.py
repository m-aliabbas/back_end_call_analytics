import pymongo

class Mongo_DB:
    def __init__(self,address='mongodb://localhost:27017/',
                 db_name='call_analytics_tool',
                 collection_name='call_record',):
        self.myclient = pymongo.MongoClient(address)
        self.mydb = self.myclient[db_name]
        self.collection = self.mydb[collection_name]
    
    def insert(self,data):
        # file_id = data['file_id']
        # files_id = "".join(self.data.keys())
        x = self.collection.insert_one(data)
        return x
    
    def check_if_exists(self,file_id):
        if self.collection.find_one(file_id) is None:
            return False
        return True

    def find(self,file_id=None,cols=[]):
        if file_id is not None:
            return list(self.collection.find(file_id,cols))
        else:
            return list(self.collection.find({},cols))
        
    
    def empty(self):
        x = self.collection.drop()
        return x