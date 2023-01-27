from db_management.DBManager import DBManager
import logging
from dotenv import load_dotenv
import os
import pandas as pd
from db_management import db_utils

def create_database(db_manager, db_name):
    '''
    This functions allows to create a new database

    PARAMETERS
    db_manager -> An instance of class DBManager needed to connect to the database
    db_connection_data -> A dictionary containing the database connection parameters (keys: host, db_name, user, psw)
    '''
    #Connect to mysql DBMS and create new database
    connection = db_manager.get_connection()
    db_utils.create_database(connection=connection,db_name=db_name)
    db_manager.release_connection(connection)
    


def export_data_to_db(db_manager, data):
    '''
    This function allows to create the tables needed to store the data in the database.
    N.B. This part is done in this script just to speed up the process. However, it can be done directly in mysql during the
    database design phase.

    PARAMETERS
    db_manager -> Aninstance of class DBManager needed to manage the database connection
    db_connection_data -> A di
    data -> A dictionary containing the data to be stored in the database. The key should represent the name of the table, whereas the value
            is a Pandas DataFrame containing the data.
    '''

    create_persons_query = """CREATE TABLE IF NOT EXISTS person(
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                first_name VARCHAR(20) NOT NULL,
                                last_name VARCHAR(20) NOT NULL,
                                email VARCHAR(254) NOT NULL,
                                gender VARCHAR(16) NOT NULL,
                                ip_address VARCHAR(15) NOT NULL
                            );"""

    create_countries_query = """CREATE TABLE IF NOT EXISTS country(
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                person_id INT NOT NULL,
                                country CHAR(2) NOT NULL,
                                FOREIGN KEY (id) REFERENCES person(id)
                            );"""

    insert_person_query = """INSERT INTO person (id, first_name, last_name, email, gender, ip_address)
                             VALUES (%s, %s, %s, %s, %s, %s)
                          """
    insert_country_query = """INSERT INTO country (id, person_id, country)
                             VALUES (%s, %s, %s)
                          """
    #Connect to the database and insert the data
    connection = db_manager.get_connection()
    
    #Create tables
    db_utils.execute_push_queries(connection = connection, queries = [create_persons_query,create_countries_query])
    
    #Insert new data
    tuples = list(data["person"].itertuples(index=False, name=None)) #Convert DataFrame to list of tuples
    db_utils.execute_list_query(connection,insert_person_query,tuples)
    tuples = list(data["country"].itertuples(index=False, name=None)) #Convert DataFrame to list of tuples
    db_utils.execute_list_query(connection,insert_country_query,tuples)

    db_manager.release_connection(connection)

        

    
def init_db():    
    persons_data_file = "../data/persons.csv"
    countries_data_file = "../data/countries.csv"

    load_dotenv() #Allows to load the variables present in the .env file
    db_connection_data = {"host":os.environ.get('db_hostname'),
                          "db_name":os.environ.get('db_name'),
                          "user":os.environ.get('db_user'),
                          "psw":os.environ.get('db_psw')}
    db_manager = DBManager(db_connection_data=db_connection_data,is_new=True)
    
    logging.basicConfig(filename=os.path.join("logs",'logs.log'), encoding='utf-8')

    try:
        create_database(db_manager=db_manager, db_name = db_connection_data["db_name"])
        #Change DBManager settings to connect to the newly created database
        db_manager.set_database(new_database=db_connection_data["db_name"])
        logging.info("Database created successfully")
    except Exception as e:
        logging.error(e)
        return
    #Load data from file into pandas dataframe
    persons_df = pd.read_csv(persons_data_file,sep=",", keep_default_na=False)
    countries_df = pd.read_csv(countries_data_file,sep=",", keep_default_na=False)
    
    data = {"person": persons_df,
            "country": countries_df}
    try:
        export_data_to_db(db_manager=db_manager, data=data)
        logging.info("Data exported successfully")
    except Exception as e:
        logging.error(e)
        return
    

if __name__ == "__main__":
    init_db()