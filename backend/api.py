from fastapi import FastAPI
from dotenv import load_dotenv
import sqlalchemy
import os
import api_functionalities
import pandas as pd
import ipaddress
import re

app = FastAPI()
load_dotenv()
pd.set_option("display.precision", 2)
db_connection_data = {"host":os.environ.get('db_hostname'),
                        "db_name":os.environ.get('db_name'),
                        "user":os.environ.get('db_user'),
                        "psw":os.environ.get('db_psw')}
engine = sqlalchemy.create_engine(f'mysql+mysqldb://{db_connection_data["user"]}:{db_connection_data["psw"]}@{db_connection_data["host"]}/{db_connection_data["db_name"]}') # Connect to database

def validate_ip_address(ip_address):
    '''
        This function allows to check whether a given ip address is valid or not

        PARAMETERS
        ip_address -> A string representing an ip_address

        RETURNS
        True if the ip_address is valid
    '''
    try:
        ipaddress.ip_address(ip_address) #Raise a ValueError exception if the given ip_address is not valid
        return True
    except ValueError:
        return False

@app.get("/create_person")
def create_person(first_name: str, last_name: str, email: str, gender: str, ip_address: str, country: str):
    # Checking formats to ensure that the given parameters are acceptable before putting them in the database
    if not first_name or len(first_name) > 30 :
        return "Invalid parameter 'first_name'. This parameter cannot be empty and should be a string of at most 30 characters long."
    elif not last_name or len(last_name) > 30 :
        return "Invalid parameter 'last_name'. This parameter cannot be empty and should be a string of at most 30 characters long."
    elif not email or len(email) > 254 or not re.match(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*@[a-zA-Z0-9]+[\[.]?[a-zA-Z0-9-]+]*\.[a-zA-Z]{2,4}$", email):
        return "Invalid parameter 'email'. This parameter cannot be empty, should be a string of at most 254 characters long and should meet the usual email format requirements."
    elif not gender or len(gender) > 20 :
        return "Invalid parameter 'gender'. This parameter cannot be empty and should be a string of at most 20 characters."
    elif not ip_address or not validate_ip_address(ip_address) :
        return "Invalid parameter 'ip_address'. This parameter cannot be empty and should represent a valid IPv4 address."
    elif not country or len(country) > 2 :
        return "Invalid parameter 'country'. This parameter cannot be empty and should be a string of at most 2 characters long."

    api_functionalities.create_new_person(engine, first_name, last_name, email, gender, ip_address, country)
    return "Person created successfully."

@app.get("/get_people_by_country")
def get_people_by_country(country: str):
    # Checking formats to ensure that the given parameters are acceptable before putting them in the database
    if not country or len(country) > 2 :
        return "Invalid parameter 'country'. This parameter cannot be empty and should be a string of at most 2 characters long."

    df = api_functionalities.get_people_by_country(engine, country)
    return df.to_dict()

@app.get("/get_people_count_by_country")
def get_people_count_by_country(country: str):
    # Checking formats to ensure that the given parameters are acceptable before putting them in the database
    if not country or len(country) > 2 :
        return "Invalid parameter 'country'. This parameter cannot be empty and should be a string of at most 2 characters long."

    count = api_functionalities.get_people_count_by_country(engine, country)
    return f"The number of users for country {country} is {count}."

@app.get("/get_people_gender_distribution")
def get_people_gender_distribution():
    df = api_functionalities.get_people_gender_distribution(engine)
    return df.to_dict()

@app.get("/get_ip_address_distribution_by_class")
def get_ip_address_distribution_by_class():
    df = api_functionalities.get_ip_address_distribution_by_class(engine)
    return df.to_dict()

@app.get("/get_most_common_domain")
def get_most_common_domain():
    domains, count = api_functionalities.get_most_common_domain(engine)
    return f"The most commons domains are {domains} which occurr {count} times each."

@app.get("/get_country_domain_correlation")
def get_country_domain_correlation():
    correlation = api_functionalities.get_country_domain_correlation(engine)
    return "The correlation between Country and Domain is {:.3f}".format(correlation)

@app.get("/get_gender_domain_correlation")
def get_gender_domain_correlation():
    correlation = api_functionalities.get_gender_domain_correlation(engine)
    return "The correlation between Gender and Domain is {:.3f}".format(correlation)

@app.get("/get_common_email_patterns")
def get_common_email_patterns():
    df = api_functionalities.get_common_email_patterns(engine)
    return df.to_dict()

@app.get("/get_gender_country_correlation")
def get_gender_country_correlation():
    correlation = api_functionalities.get_gender_country_correlation(engine)
    return "The correlation between Gender and Country is {:.3f}".format(correlation)

@app.get("/get_gender_distribution_by_country")
def get_gender_distribution_by_country():
    df = api_functionalities.get_gender_distribution_by_country(engine)
    return df.to_dict()