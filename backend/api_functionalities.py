from db_management.db_entities import Person, Country
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import select, func
import pandas as pd
import scipy.stats as stats
import numpy as np
 

def compute_cramer_V_correlation(contingency_table):
    '''
        This function allows to compute the correlation between nominal variables

        PARAMETERS
        df -> A Pandas Dataframe representing a contingency table
    '''

    #Compute Chi-square statistic
    chi2 = stats.chi2_contingency(contingency_table, correction=False)[0]
    #Compute number of observations
    n = np.sum(contingency_table)
    minDim = min(contingency_table.shape)-1

    #Compute Cramer's V with correction
    k = contingency_table.shape[1]
    r = contingency_table.shape[0]
    phi2_tilde = max(0,chi2/n - (k -1)*(r -1)/(n-1))
    k_tilde = k - ((k-1)^2)/(n-1)
    r_tilde = r - ((r-1)^2)/(n-1)

    cramerV = np.sqrt(phi2_tilde/min(k_tilde-1,r_tilde-1))

    return cramerV

def create_new_person(engine, first_name, last_name, email, gender, ip_address, country):
    '''
        This function allows to create a new person and insert it into the database

        PARAMETERS
        engine -> A sqlalchemy engine to interact with the database
        first_name, last_name, email, gender, ip_address, country -> The information concerning the new person (each being a string)

        RETURNS
        Nothing if the operation was successfull, otherwise an Exception is thrown
    '''    
    
    #Format strings before putting them in the database to keep format consistency
    first_name = first_name.title() #Put first letter in upper case and everything else lower case
    last_name = last_name.title()   #Put first letter in upper case and everything else lower case
    email = email.lower()           #Lower case
    gender = gender.title()         #Put first letter in upper case and everything else lower case
    country = country.upper()       #Upper case

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

        PARAMETERS
        engine -> A sqlalchemy engine to interact with the database
        country -> A string representing a country

        RETURNS
        A Pandas Dataframe containing the information of all people from the specified country
    '''
    country = country.upper() #Put country in upper case

    # Create a new instance of Person and push it to the database
    Session = sessionmaker(engine, expire_on_commit = False)
    with Session() as session:
        results = session.query(Person, id == Country.person_id).join(Country).filter(Country.country == country).all()
    
    return pd.DataFrame.from_records([p[0].to_dict() for p in results])

def get_people_count_by_country(engine, country):
    '''
        This function allows to obtain the number of users given a country

        PARAMETERS
        engine -> A sqlalchemy engine to interact with the database
        country -> A string representing a country

        RETURNS
        An integer representing the count of all people from the specified country
    '''
    country = country.upper() #Put country in upper case

    Session = sessionmaker(engine, expire_on_commit = False)
    query  = select(func.count("*")).select_from(Person).join(Country).filter(Person.id == Country.person_id, Country.country == country)
    with Session() as session:        
        count = session.execute(query).one()
    
    return count[0]

def get_people_gender_distribution(engine):
    '''
        This function allows to obtain the distribution of persons over genders

        PARAMETERS
        engine -> A sqlalchemy engine to interact with the database

        RETURNS
        A Pandas Dataframe containing the information concerning the gender distribution
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

        PARAMETERS
        engine -> A sqlalchemy engine to interact with the database

        RETURNS
        A Pandas Dataframe containing the information concerning the ip address class distribution
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

def get_most_common_domain(engine):
    '''
        This function allows to obtain the most common email domain

        PARAMETERS
        engine -> A sqlalchemy engine to interact with the database

        RETURNS
        A list of the most common domains along with the maximum frequency
    '''
    
    Session = sessionmaker(engine, expire_on_commit = False)
    
    # Retrieve all emails from the database
    with Session() as session:     
        results = session.query(Person.email).all()
    
    domains_count = {}
    for result in results:
        domain = result[0].split("@")[1]
        if domain in domains_count.keys():
            domains_count[domain] += 1
        else:
            domains_count[domain] = 1
        
    max_count = max(domains_count.values())
    most_common_domains = [domain for domain, count in domains_count.items() if count == max_count]
    
    return most_common_domains, max_count

def get_country_domain_correlation(engine):
    '''
        This function allows to obtain the country-domain correlation

        PARAMETERS
        engine -> A sqlalchemy engine to interact with the database

        RETURNS
        The correlation between country and domain computed using Cramer-V method
    '''
    
    Session = sessionmaker(engine, expire_on_commit = False)
    
    # Retrieve email and country for each person
    with Session() as session:     
        results = session.query(Person.email, Country.country).join(Country).filter(Person.id == Country.person_id).all()
    
    # Modify the returned data to obtain the pairs <domain, country>
    df = pd.DataFrame.from_records([{"Domain":r[0], "Country": r[1]} for r in results])
    df["Domain"] = [d.split("@")[1] for d in df["Domain"]]
    
    contingency_table = pd.crosstab(df["Domain"], df["Country"]).to_numpy()

    return compute_cramer_V_correlation(contingency_table)

def get_gender_domain_correlation(engine):
    '''
        This function allows to obtain the gender-domain correlation

        PARAMETERS
        engine -> A sqlalchemy engine to interact with the database

        RETURNS
        The correlation between country and domain computed using Cramer-V method
    '''
    
    Session = sessionmaker(engine, expire_on_commit = False)
    
    # Retrieve email and gender for each person
    with Session() as session:     
        results = session.query(Person.email, Person.gender).all()
    
    # Modify the returned data to obtain the pairs <domain, gender>
    df = pd.DataFrame.from_records([{"Domain":r[0], "Gender": r[1]} for r in results])
    df["Domain"] = [d.split("@")[1] for d in df["Domain"]]
    
    contingency_table = pd.crosstab(df["Domain"], df["Gender"]).to_numpy()

    return compute_cramer_V_correlation(contingency_table)

def get_common_email_patterns(engine):
    '''
        This function allows to obtain the common email patters (e.g., name.surname)

        PARAMETERS
        engine -> A sqlalchemy engine to interact with the database

        RETURNS
        A DataFrame with the email patterns count
    '''
    
    Session = sessionmaker(engine, expire_on_commit = False)
    
    # Retrieve email and gender for each person
    with Session() as session:     
        results = session.query(Person.first_name, Person.last_name, Person.email).all()
    
    # Modify the returned data to obtain the tuple <first_name, last_name, email>
    df = pd.DataFrame.from_records([{"first_name":r[0], "last_name": r[1], "email": r[2]} for r in results])
    df["email"] = [d.split("@")[0] for d in df["email"]]
    
    df["first_name"] = df["first_name"].str.lower()
    df["last_name"] = df["last_name"].str.lower()

    #Compute most common email patterns
    df['firstlast'] = df.apply(lambda x: x["email"].startswith(x["first_name"]+x["last_name"]), axis=1)
    df['lastfirst'] = df.apply(lambda x: x["email"].startswith(x["last_name"]+x["first_name"]), axis=1)
    df['first.last'] = df.apply(lambda x: x["email"].startswith(x["first_name"]+'.'+x["last_name"]), axis=1)
    df['last.first'] = df.apply(lambda x: x["email"].startswith(x["last_name"]+'.'+x["first_name"]), axis=1)
    df['flast'] = df.apply(lambda x: x["email"].startswith(x["first_name"][0]+x["last_name"]), axis=1)
    df['f.last'] = df.apply(lambda x: x["email"].startswith(x["first_name"][0]+'.'+x["last_name"]), axis=1)
    df['firstl'] = df.apply(lambda x: x["email"].startswith(x["first_name"]+x["last_name"][0]) and not x["firstlast"], axis=1)
    df['first.l'] = df.apply(lambda x: x["email"].startswith(x["first_name"]+'.'+x["last_name"][0]) and not x['first.last'], axis=1)
    df['first'] = df.apply(lambda x: x["email"].startswith(x["first_name"]) and not x['first.last'] and not x['firstlast'] and not x['firstl'] and not x['first.l'] and not x["flast"], axis=1)
    df['last'] = df.apply(lambda x: x["email"].startswith(x["last_name"]) and not x['last.first'] and not x['lastfirst'], axis=1)
    
    return (df.iloc[:,3:].sum().sort_values(ascending=False)).to_frame(name="Count")
    

def get_gender_country_correlation(engine):
    '''
        This function allows to obtain the gender-country correlation

        PARAMETERS
        engine -> A sqlalchemy engine to interact with the database

        RETURNS
        The correlation between country and domain computed using Cramer-V method
    '''
    
    Session = sessionmaker(engine, expire_on_commit = False)
    
    # Retrieve country and gender for each person
    with Session() as session:     
        results = session.query(Person.gender, Country.country).join(Country).filter(Person.id == Country.person_id).all()

    df = pd.DataFrame.from_records([{"Gender": r[0], "Country": r[1] } for r in results])
    
    contingency_table = pd.crosstab(df["Gender"], df["Country"]).to_numpy()

    return compute_cramer_V_correlation(contingency_table)

def get_gender_distribution_by_country(engine):
    '''
        This function allows to obtain the gender distribution over countries

        PARAMETERS
        engine -> A sqlalchemy engine to interact with the database

        RETURNS
        A Pandas Dataframe containing the information concerning the gender distribution
    '''
    
    Session = sessionmaker(engine, expire_on_commit = False)
    
    with Session() as session:        
        results = session.query(Country.country,Person.gender,func.count("*")).join(Country).filter(Person.id == Country.person_id).group_by(Country.country,Person.gender).all()
    
    df = pd.DataFrame.from_records([{"Country": p[0], "Gender": p[1], "Count": p[2]} for p in results])
    counts = df.groupby("Country")['Count'].sum()
    
    df["Distribution (%)"] = df.apply(lambda x: x["Count"]/counts[x["Country"]]* 100, axis=1)

    return df.sort_values("Country")

