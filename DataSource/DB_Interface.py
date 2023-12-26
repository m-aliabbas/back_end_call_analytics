from .MySQL_DB import MYSQL_DB


class DB_Interface:
    def __init__(self):
        # Create an instance of the MYSQL_DB class for database interaction
        self.DB = MYSQL_DB()
        
        # Check if the database connection was successful
        if self.DB is None:
            print('Database Not Connected')
            
    def insert(self, filename, data):
        # Call the insert method of the MYSQL_DB class to insert data into the database
        self.DB.insert(filename, data)
    
    def get_all(self):
        # Retrieve all data from the database using the get_all_data method of the MYSQL_DB class
        results = self.DB.get_all_data()
        results_list = []
        
        for result in results:
            # Convert the data stored in the database to its original form
            results_list.append(eval(result[2]))
        
        return results_list


# Create an instance of the DB_Interface class
db_interface = DB_Interface()

# Retrieve all data from the database
db_interface.get_all()
