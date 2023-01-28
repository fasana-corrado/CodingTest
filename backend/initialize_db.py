import logging
from dotenv import load_dotenv
import os
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import Session
import db_management.db_entities as db_entities

def export_data_to_db(engine, data):
    '''
    This function allows to create the tables needed to store the data in the database.
    And to export data from the csv files to the relational database.

    PARAMETERS
    data -> A dictionary containing the data to be stored in the database. The key should represent the name of the table, whereas the value
            is a Pandas DataFrame containing the data.
    '''
    db_entities.Base.metadata.create_all(engine) #Creates tables

    #Create objects
    persons_tuples = data["person"].itertuples(index=False, name=None)
    countries_tuples = data["country"].itertuples(index=False, name=None)

    persons = list()
    countries = list()

    persons = [db_entities.Person(p[0], p[1], p[2], p[3], p[4], p[5]) for p in persons_tuples]
    countries = [db_entities.Country(c[0], c[1], c[2]) for c in countries_tuples]
    
    #Push everything to the database
    with Session(engine) as session:
        session.add_all(persons+countries)
        session.commit()
    
    
    '''
    #Insert new data
    tuples = list(data["person"].itertuples(index=False, name=None)) #Convert DataFrame to list of tuples
    db_utils.execute_list_query(connection,insert_person_query,tuples)
    tuples = list(data["country"].itertuples(index=False, name=None)) #Convert DataFrame to list of tuples
    db_utils.execute_list_query(connection,insert_country_query,tuples)

    db_manager.release_connection(connection)'''

        

def init_db():
    '''
    This function manages the creation of the database and data transfer from cvs files to the relational database
    '''  
    persons_data_file = "../data/persons.csv"
    countries_data_file = "../data/countries.csv"
    
    load_dotenv() #Allows to load the variables present in the .env file
    db_connection_data = {"host":os.environ.get('db_hostname'),
                          "db_name":os.environ.get('db_name'),
                          "user":os.environ.get('db_user'),
                          "psw":os.environ.get('db_psw')}
   
    logging.basicConfig(filename=os.path.join("logs",'logs.log'), encoding='utf-8', level=logging.DEBUG)

    try:
        engine = sqlalchemy.create_engine(f'mysql+mysqldb://{db_connection_data["user"]}:{db_connection_data["psw"]}@{db_connection_data["host"]}') # connect to server
        with engine.connect() as connection:
            connection.execute(sqlalchemy.text(f"CREATE DATABASE IF NOT EXISTS {db_connection_data['db_name']}"))
        engine = sqlalchemy.create_engine(f'mysql+mysqldb://{db_connection_data["user"]}:{db_connection_data["psw"]}@{db_connection_data["host"]}/{db_connection_data["db_name"]}') # connect to server
        logging.info("Database created successfully")
    except sqlalchemy.exc.SQLAlchemyError as e:
        logging.error(e)
        return

    #Load data from file into pandas dataframe
    persons_df = pd.read_csv(persons_data_file,sep=",", keep_default_na=False)
    countries_df = pd.read_csv(countries_data_file,sep=",", keep_default_na=False)
    
    data = {"person": persons_df,
            "country": countries_df}
    try:
        export_data_to_db(engine = engine, data = data)
        logging.info("Data exported successfully from csv files to relational database")
    except sqlalchemy.exc.SQLAlchemyError as e:
        logging.error(e)
        return
    

if __name__ == "__main__":
    init_db()