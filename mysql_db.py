import mysql.connector, logging
import os

class MySQL_DB:
    def __init__(self, host, user, password, database):
        logging.info("mysql db connection")
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = mysql.connector.connect(host = host,
             user = user,
             password = password,
             database = database,
             auth_plugin='mysql_native_password')
        
    def close_connection(self):
        logging.info("db connection closing function")
        self.connection.close()

    def get_databases(self):
        logging.info("get databases function")
        cursor = self.connection.cursor()
        query = """SHOW DATABASES;"""
        cursor.execute(query)
        databases = cursor.fetchall()
        return databases
    
    def get_all_items(self,table):
        logging.info("get all items function")
        cursor = self.connection.cursor()

        # Execute the query
        cursor.execute('SELECT * FROM {}.{};'.format(self.database,table))
        records = cursor.fetchall()

        # Convert list of tuples into list of lists
        list_of_lists = [list(elem) for elem in records]

        # all records
        records = []

        # Creating dictionary
        for row in list_of_lists:
            data = {}
            data['id'] = row[0]
            data['name'] = row[1]
            data['genre'] = row[2]
            data['year'] = row[3]
            records.append(data)

        return records
    
    def get_items_by_genre(self,table,genre):
        logging.info("get items by make function")
        cursor = self.connection.cursor()

        # Execute the query
        cursor.execute("""SELECT * FROM {}.{} WHERE genre='{}';""".format(self.database,table,genre))
        records = cursor.fetchall()

        # Convert list of tuples into list of lists
        list_of_lists = [list(elem) for elem in records]

        # all records
        records = []

        # Creating dictionary
        for row in list_of_lists:
            data = {}
            data['id'] = row[0]
            data['name'] = row[1]
            data['genre'] = row[2]
            data['year'] = row[3]
            records.append(data)

        return records
    
    def get_items_by_id(self,table,id):
        logging.info("get items by id function")
        cursor = self.connection.cursor()

        # Execute the query
        cursor.execute("""SELECT * FROM {}.{} WHERE id='{}';""".format(self.database,table,id))
        records = cursor.fetchall()

        # Convert list of tuples into list of lists
        list_of_lists = [list(elem) for elem in records]

        # all records
        records = []

        # Creating dictionary
        for row in list_of_lists:
            data = {}
            data['id'] = row[0]
            data['name'] = row[1]
            data['genre'] = row[2]
            data['year'] = row[3]
            records.append(data)

        return records[0]
    
    def get_record_count(self,table):
        logging.info("get record count function")
        cursor = self.connection.cursor()
        query = """SELECT COUNT(*) FROM {}.{};""".format(self.database,table)
        cursor.execute(query)
        count = cursor.fetchall()
        return count[0][0]
    
    def add_record(self,table_name,name,genre,year):
        logging.info("add record function")
        cursor = self.connection.cursor()
        # Execute the query
        query = """INSERT INTO `{}` (name,genre,year) 
                    VALUES (%s,%s,%s)""".format(table_name)
        val = (name,genre,year)
        
        cursor.execute(query, val)
        self.connection.commit()
        logging.info("new record entered successfully")

    def check_record_exists(self,table_name,id):
        logging.info("check record exists function")
        cursor = self.connection.cursor()
        # Execute the query
        query = """SELECT * FROM `{}` WHERE id=%s""".format(table_name)
        val = (id,)
        
        cursor.execute(query, val)
        records = cursor.fetchall()

        if len(records) > 0:
            return True
        elif len(records) == 0:
            return False
        
    def update_record(self,table_name,name,genre,year):
        logging.info("update record function")
        cursor = self.connection.cursor()
        # Execute the query
        query = """UPDATE `{}` SET name=%s,genre=%s,year=%s""".format(table_name)
        val = (name,genre,year)
        
        cursor.execute(query, val)
        self.connection.commit()
        logging.info("record updated successfully")

    def delete_record(self,table_name,id):
        logging.info("delete record function")
        cursor = self.connection.cursor()
        # Execute the query
        query = """DELETE FROM `{}` WHERE id=%s""".format(table_name)
        val = (id,)
        
        cursor.execute(query, val)
        self.connection.commit()
        logging.info("record deleted successfully")
        
if __name__ == '__main__':

    # read environment variables
    DB_HOST = os.environ['DB_HOST']
    DB_USER = os.environ['DB_USER']
    DB_PASSWORD = os.environ['DB_PASSWORD']
    DB_NAME = os.environ['DB_NAME']
    DB_TABLE = os.environ['DB_TABLE']

    # db_obj = MySQL_DB(DB_HOST,DB_USER,DB_PASSWORD,DB_NAME)
    # db_obj.add_record(DB_TABLE,"Friends","Sitcom","1994")
    