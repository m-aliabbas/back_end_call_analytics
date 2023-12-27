import pymongo

class Mongo_DB:
    def __init__(self,address,db_name,collection_name,):
        self.myclient = pymongo.MongoClient(address)
        self.mydb = self.myclient[db_name]
        self.collection = self.mydb[collection_name]
    
    def insert(self,data):
        x = self.collection.insert_one(data)
        return x
    
    def replace_insert(self, data):
        result = self.collection.replace_one({}, data, upsert=True)
        return result
    
    def check_if_exists(self, file_id):
        query = {"file_id": file_id}  # Adjust the field name if necessary
        if self.collection.find_one(query) is None:
            return False
        return True

    
    def check_if_exists_logs(self,file):
        if self.collection.find_one({ "caller_id": file }) is None:
            return False
        return True
    
    def check_if_exists_states(self,stated):
        if self.collection.find_one({ "states": stated }) is None:
            return False
        return True

    def find(self,file_id=None,cols=[]):
        if file_id is not None:
            return list(self.collection.find(file_id,cols))
        else:
            return list(self.collection.find({},cols))
        
    def find_processor(self, file_id=None, cols=[]):
        query = {"already_processed": False}
        if file_id is not None:
            query["file_id"] = file_id  # Adjust as per your schema
        return list(self.collection.find(query, cols))


    def find_disposition(self, state, file_id=None, cols=[]):
        query = {}

        if state.lower() != "all":
            query["current_state"] = state
        
        if file_id is not None:
            query["file_id"] = file_id

        results = self.collection.find(query, cols)

        # Extracting disposition values from the results
        disposition_values = [result.get("disposition") for result in results]

        return disposition_values
    
    # def find_new_disposition(self, area_states, dispos, cols=[]):
    #     query = {
    #         "flag": False  # Add the condition for the "flag" field
    #     }

    #     if isinstance(dispos, list):
    #         query["disposition"] = {"$in": dispos}
    #     elif dispos.lower() != "all":
    #         query["disposition"] = dispos

    #     if area_states:
    #         query["area_state"] = {"$in": area_states}

    #     projection = {"area_state": 1}  # Include area_state in the projection
    #     if cols:
    #         projection.update({col: 1 for col in cols})  # Include additional columns in the projection

    #     results = self.collection.find(query, projection)

    #     return results
    def find_new_disposition_sliced(self, area_states, dispos, area_exclude=False, disp_exclude=False, cols=[],
                                    start_index=0,num_rows=20):
        query = {
            "flag": False  # Add the condition for the "flag" field
        }
        # Add or invert condition for disposition if dispos is not empty
        if dispos:
            if isinstance(dispos, list):
                if disp_exclude:
                    query["disposition"] = {"$nin": dispos}
                else:
                    query["disposition"] = {"$in": dispos}
            elif dispos.lower() != "all":
                if disp_exclude:
                    query["disposition"] = {"$ne": dispos}
                else:
                    query["disposition"] = dispos

        # Add or invert condition for area_state if area_states is not empty
        if area_states:
            if area_exclude:
                query["area_state"] = {"$nin": area_states}
            else:
                query["area_state"] = {"$in": area_states}

        # Set up projection
        projection = {"area_state": 1}  # Include area_state in the projection
        if cols:
            projection.update({col: 1 for col in cols})  # Include additional columns in the projection

        # Execute query
        results = self.collection.find(query, projection).skip(start_index).limit(num_rows)

        return list(results)
    
    def find_new_disposition(self, area_states, dispos, area_exclude=False, disp_exclude=False, cols=[]):
        query = {
            "flag": False  # Add the condition for the "flag" field
        }
        # Add or invert condition for disposition if dispos is not empty
        if dispos:
            if isinstance(dispos, list):
                if disp_exclude:
                    query["disposition"] = {"$nin": dispos}
                else:
                    query["disposition"] = {"$in": dispos}
            elif dispos.lower() != "all":
                if disp_exclude:
                    query["disposition"] = {"$ne": dispos}
                else:
                    query["disposition"] = dispos

        # Add or invert condition for area_state if area_states is not empty
        if area_states:
            if area_exclude:
                query["area_state"] = {"$nin": area_states}
            else:
                query["area_state"] = {"$in": area_states}

        # Set up projection
        projection = {"area_state": 1}  # Include area_state in the projection
        if cols:
            projection.update({col: 1 for col in cols})  # Include additional columns in the projection

        # Execute query
        results = self.collection.find(query, projection)

        return results
    
    def update_flag(self, record_id):
        # Update the "flag" field to True for the given record ID
        self.collection.update_one({"_id": record_id}, {"$set": {"flag": True}})

    def update_already_processed_flag(self,record_id):
        print('Incoming record ID',record_id)
        self.collection.update_one({"file_id": record_id}, {"$set": {"already_processed": True}}) 

    def find_new_disposition_area(self, cols=[]):
        query = {}
        results = list(self.collection.find(query, projection={col: 1 for col in cols}))
        return results

    
    def find_logs_range(self, state, start_index, num_rows, file_id=None, cols=[]):
        # Create a query based on the state
        query = {"current_state": state} if state and state != "all" else {}

        # Use skip() and limit() to implement pagination
        results = self.collection.find(query, cols).skip(start_index).limit(num_rows)

        return list(results)

        
    def count_states(self,state):
        return self.collection.count_documents({"current_state": state})

    def count_all_states(self,):
        return self.collection.count_documents({})
        
    
    def empty(self):
        x = self.collection.drop()
        return x