'''
This file contains a set of methods that were used in the initial version of the software, to interact with the database.
After switching to sqlalchemy, this functions are not needed anymore.
'''

from mysql.connector import  Error

def create_database(connection, db_name):
    '''
        This function allows to create a new database.

        PARAMETERS
        connection -> A connection to the DBMS where the database should be created.
        db_name -> A string indicating the name of the new database.
    '''
    
    create_db_query = f"""CREATE DATABASE IF NOT EXISTS {db_name}"""
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_db_query)
    except Error as e:
        raise Exception("Unable to create database")

def execute_push_queries(connection, queries):
    '''
        This function allows to execute a set of push queries on the database.
        It can be used to create tables.

        PARAMETERS
        connection -> A connection to the database where queries should be executed.
        queries -> A list of queries to be executed.
    '''
    
    try:
        with connection.cursor() as cursor:
            for query in queries:
                cursor.execute(query)
            connection.commit()
    except Error as e:
        raise Exception("Unable to execute push queries")

def execute_list_query(connection, query, values):
    '''
        This function allows to execute a push query on the database using a list of values.
        It can be used to insert new data.

        PARAMETERS
        connection -> A connection to the database where queries should be executed.
        query -> A string representing the query to be executed.
        values -> A list of tuples to be used in the query.
    '''

    try:
        with connection.cursor() as cursor:
            cursor.executemany(query,values)
            connection.commit()
    except Error as e:
        raise Exception(e)

def execute_pull_query(connection, query):
    '''
        This function allows to execute a pull query on the database.
        It could be used to extract values from tables.

        PARAMETERS
        connection -> A connection to the database where queries should be executed.
        query -> A string representing the query to be executed.
        
        RETURNS
        The result of the specific query.
    '''
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result =  cursor.fetchall()
            return result
    except Error as e:
        raise Exception("Unable to execute pull query")

