import mysql.connector


class MYSQL_DB:
    def __init__(self, host="localhost", user="root", password="", database="idrak_analytics") -> None:
        self.mydb = None
        try:
            # Initialize connection parameters
            self.host = host
            self.user = user
            self.password = password
            self.database = database
            
            # Connect to the MySQL database
            self.mydb = mysql.connector.connect(
                host='localhost',
                user='root',
                password='',
                database='idrak_analytics'
            )
            
            # Set the table name and create a cursor object
            self.table_name = "calls_records"
            self.mycursor = self.mydb.cursor()
        except Exception as e:
            print(e)
        
        # Check if the database connection was successful
        if self.mydb is None:
            print('Database connection Error')

    def insert(self, filename, data):
        # Prepare the SQL query
        sql = "INSERT INTO calls_records (call_file_id, call_data) VALUES (%s, %s)"
        
        # Set the values to be inserted into the database
        values = (filename, str(data))
        
        # Execute the query and commit the changes
        self.mycursor.execute(sql, values)
        self.mydb.commit()

    def get_all_data(self):
        # Execute a SELECT query to fetch all rows from the table
        self.mycursor.execute("SELECT * FROM calls_records")
        
        # Fetch all the results
        result = self.mycursor.fetchall()
        # Return the fetched result
        return result
