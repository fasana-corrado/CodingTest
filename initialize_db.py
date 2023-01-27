from DBManager import DBManager
import logging
from dotenv import load_dotenv
import os
import pandas as pd

def create_database(db_manager):
    create_db_query = "CREATE DATABASE IF NOT EXISTS testDB"
    #Connect to mysql DBMS and create new database
    try:
        connection = db_manager.getConnection(hostname=os.environ.get('db_hostname'), dbname=None, user=os.environ.get('db_user'), psw=os.environ.get('db_psw'))
        db_manager.create_database(connection=connection,statement=create_db_query)
        db_manager.releaseConnection(connection)
    except Exception as e:
        raise Exception(e)

def export_data_to_db(db_manager, data):
    create_persons_query = """CREATE TABLE person(
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                first_name VARCHAR(20) NOT NULL,
                                last_name VARCHAR(20) NOT NULL,
                                email VARCHAR(254) NOT NULL,
                                gender VARCHAR(6) NOT NULL,
                                ip_address VARCHAR(15) NOT NULL,
                                CHECK (email RLIKE '^[a-zA-Z]@[a-zA-Z0-9]\\.[a-z,A-Z]{2,4}')),
                                CHECK (ip_address RLIKE '^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$'),
                                CHECK (gender IN ('male','female'))

                            );"""
    create_persons_query = """CREATE TABLE country(
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                person_id INT NOT NULL,
                                country CHAR(2) NOT NULL,
                                FOREIGN KEY (id) REFERENCES person(id)
                            );"""
    ip_address_constraint = """
    """
    for table in data.keys():
        return

    
def init_db():    
        
    persons_data_file = "data/persons.csv"
    countries_data_file = "data/countries.csv"
    db_manager = DBManager()
    
    logging.basicConfig(filename='logs.log', encoding='utf-8', level=logging.DEBUG)
    load_dotenv() #Allows to load the variables present in the .env file

    try:
        create_database(db_manager=db_manager)
        logging.info("Database created successfully")
    except Exception as e:
        logging.error(e)
        
    #Load data from file into pandas dataframe
    persons_df = pd.read_csv(persons_data_file,sep=",")
    countries_df = pd.read_csv(countries_data_file,sep=",")
    
    data = {"person": persons_df,
            "country": countries_df}
    try:
        export_data_to_db(db_manager=db_manager, data=data)
        logging.info("Data exported successfully")
    except Exception as e:
        logging.error(e)
    

if __name__ == "__main__":
    init_db()