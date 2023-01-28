from backend.db_management.db_entities import Person, Country
import re
import ipaddress
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import select, func
import pandas as pd
 

def validate_ip_address(ip_address):
    '''
        This function allows to check whether a given ip address is valid or not
    '''
    try:
        ipaddress.ip_address(ip_address) #Raise a ValueError exception if the given ip_address is not valid
        return True
    except ValueError:
        return False

def create_new_person(engine, first_name, last_name, email, gender, ip_address, country):
    '''
        This function allows to create a new person and insert it into the database
    '''
    # Checking formats to ensure that the given parameters are acceptable before putting them in the database
    if not first_name or not isinstance(first_name,str) or len(first_name) > 30 :
        raise Exception("Invalid parameter 'first_name'. This parameter cannot be empty and should be a string of at most 30 characters long.")
    elif not last_name or not isinstance(last_name,str) or len(last_name) > 30 :
        raise Exception("Invalid parameter 'last_name'. This parameter cannot be empty and should be a string of at most 30 characters long.")
    elif not email or not isinstance(email,str) or len(email) > 254 or not re.match(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*@[a-zA-Z0-9]+[\[.]?[a-zA-Z0-9-]+]*\.[a-zA-Z]{2,4}$", email):
        raise Exception("Invalid parameter 'email'. This parameter cannot be empty, should be a string of at most 254 characters long and should meet the usual email format requirements.")
    elif not gender or not isinstance(gender,str) or len(gender) > 20 :
        raise Exception("Invalid parameter 'gender'. This parameter cannot be empty and should be a string of at most 20 characters.")
    elif not ip_address or not isinstance(ip_address,str) or not validate_ip_address(ip_address) :
        raise Exception("Invalid parameter 'ip_address'. This parameter cannot be empty and should represent a valid IPv4 address.")
    elif not country or not isinstance(country,str) or len(country) > 2 :
        raise Exception("Invalid parameter 'country'. This parameter cannot be empty and should be a string of at most 2 characters long.")

    # Create a new instance of Person and push it to the database
    new_person = Person(id = None, first_name = first_name, last_name = last_name, email = email, gender = gender, ip_address = ip_address)
    new_country = Country(id = None, person_id = None, country=country)
    
    Session = sessionmaker(engine, expire_on_commit = False)
    with Session() as session:
            session.add(new_person)
            session.commit()
            new_country.person_id= new_person.id
            session.add(new_country)
            session.commit()
    
def get_people_by_country(engine, country):
    '''
        This function allows to obtain the list of users given a country
    '''
    # Checking formats to ensure that the given parameters are acceptable before putting them in the database
    if not country or not isinstance(country,str) or len(country) > 2 :
        raise Exception("Invalid parameter 'country'. This parameter cannot be empty and should be a string of at most 2 characters long.")

    # Create a new instance of Person and push it to the database
    
    Session = sessionmaker(engine, expire_on_commit = False)
    with Session() as session:
        results = session.query(Person, id == Country.person_id).join(Country).filter(Country.country == country).all()
    
    return pd.DataFrame.from_records([p[0].to_dict() for p in results])

def get_people_count_by_country(engine, country):
    '''
        This function allows to obtain the number of users given a country
    '''
    # Checking formats to ensure that the given parameters are acceptable before putting them in the database
    if not country or not isinstance(country,str) or len(country) > 2 :
        raise Exception("Invalid parameter 'country'. This parameter cannot be empty and should be a string of at most 2 characters long.")
    
    Session = sessionmaker(engine, expire_on_commit = False)
    query  = select(func.count("*")).select_from(Person).join(Country).filter(Person.id == Country.person_id, Country.country == country)
    with Session() as session:        
        count = session.execute(query).one()
    
    return count[0]

def get_people_gender_distribution(engine):
    '''
        This function allows to obtain the distribution of persons over genders
    '''
    
    Session = sessionmaker(engine, expire_on_commit = False)
    
    with Session() as session:        
        results = session.query(Person.gender,func.count("*")).group_by(Person.gender).all()
    
    df = pd.DataFrame.from_records([{"Gender":p[0], "Count": p[1]} for p in results])
    df["Distribution (%)"] = df["Count"] / df["Count"].sum() * 100
    
    return df

def get_ip_address_distribution_by_class(engine):
    '''
        This function allows to obtain the distribution of ip addresses over classes
    '''
    
    Session = sessionmaker(engine, expire_on_commit = False)
    
    with Session() as session:        
        results = session.query(Person.ip_address).all()
    
    class_count = {"A": 0, "B": 0, "C": 0, "D": 0, "E": 0,}
    for result in results:
        first_number = int(result[0].split(".")[0])
        if 0 <= first_number <= 127: 
            class_count["A"]+=1
        elif 128 <= first_number <= 191: 
            class_count["B"]+=1
        elif 192 <= first_number <= 223: 
            class_count["C"]+=1
        elif 224 <= first_number <= 239: 
            class_count["D"]+=1
        if 240 <= first_number <= 255: 
            class_count["E"]+=1
        
    df = pd.DataFrame(index = ["A", "B", "C", "D", "E"])
    df["Count"] = class_count.values()
    df["Distribution (%)"] = df["Count"] / df["Count"].sum() * 100
    
    return df
    
    