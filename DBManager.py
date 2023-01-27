from mysql.connector import connect, Error, errorcode
import logging
class DBManager:
    def __init__(self, use_logs = True):
        self.use_logs = use_logs

    def getConnection(self, hostname, dbname, user, psw):
        try:
            connection = connect(host=hostname,
                                database=dbname,
                                user=user,
                                password=psw)
            logging.info("Connection established successfully")
        except Error as e:
            if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                if self.use_logs: 
                    logging.error("Access denied, check credentials")
                raise Exception("Access denied, check credentials")
            elif e.errno == errorcode.ER_BAD_DB_ERROR:
                if self.use_logs: 
                    logging.error("The indicated database does not exist")
                raise Exception("The indicated database does not exist")
            else:
                if self.use_logs: 
                    logging.error(e)
                raise Exception(e)
        return connection

    def releaseConnection(self,connection):
        if connection:
            connection.close()
            logging.info("Closed connection")

    def create_database(self, connection, statement):
        try:
            with connection.cursor() as cursor:
                cursor.execute(statement)
        except Error as e:
            raise Exception("Unable to create database")
       
    def execute_push_queries(self, connection, queries):
        try:
            with connection.cursor() as cursor:
                for query in queries:
                    cursor.execute(query)
                connection.commit()
            logging.info("Push queries executed successfully")
        except Error as e:
            logging.error(e)
            raise Exception("Unable to execute push queries")

    def execute_list_query(connection, query, values):
        try:
            with connection.cursor() as cursor:
                cursor.executemany(query,values)
                connection.commit()
            logging.info("Push list query executed successfully")
        except Error as e:
            logging.error(e)
            raise Exception("Unable to execute push list query")

    def execute_pull_query(self, connection, query):
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result =  cursor.fetchall()
                logging.info("Pull query executed successfully")
                return result
        except Error as e:
            logging.error(e)
            raise Exception("Unable to execute pull query")

    