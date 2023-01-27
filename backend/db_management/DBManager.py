from mysql.connector import connect, Error, errorcode
class DBManager:
    '''
    This class allows to manage the connection with the database
    '''
    
    def __init__(self, db_connection_data, is_new):
        '''
            PARAMETERS
            db_connection_data -> A dictionary containing the database connection parameters (keys: host, db_name, user, psw)
            isNew -> A boolean variable indicating whether the connections is use for a database that still needs to be created
        '''
        self.host = db_connection_data["host"]
        self.database=  None if is_new else db_connection_data["db_name"]
        self.user = db_connection_data["user"]
        self.password = db_connection_data["psw"]

    def get_connection(self):
        '''
            This function allows to obtain a connection to the database.
        '''
        try:           
            #If necessary, it is possible to add a few parameters to realise a connection pooling mechanism
            connection = connect(host=self.host,
                                database=self.database,
                                user=self.user,
                                password=self.password)
        except Error as e:
            if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                raise Exception("Access denied, check credentials")
            elif e.errno == errorcode.ER_BAD_DB_ERROR:
                raise Exception("The indicated database does not exist")
            else:
                raise Exception(e)
        return connection

    def release_connection(self,connection):
        '''
            This function allows to close a database connection.

            PARAMETERS
            connection -> An instance representing a connection to the database.
        '''
        if connection:
            connection.close()

    def set_database(self, new_database):
        '''
            This function allows to update the database information. Thus, subsequent connections will be established on the newly
            specified database
        '''
        self.database = new_database