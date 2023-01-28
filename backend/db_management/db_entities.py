from sqlalchemy import String, Column, Integer, CheckConstraint, ForeignKey
from sqlalchemy.orm import relationship, DeclarativeBase, mapped_column


class Base(DeclarativeBase):
    pass

class Person(Base):
    __tablename__ = "person"

    # Defining table attributes
    id = mapped_column(Integer, autoincrement=True, primary_key=True)
    first_name  = mapped_column(String(30), nullable=False)
    last_name   = mapped_column(String(30), nullable=False)
    email  = mapped_column(String(254), CheckConstraint("email RLIKE '^[a-zA-Z0-9][a-zA-Z0-9._-]*@[a-zA-Z0-9]+[[.]?[a-zA-Z0-9-]+]*\\.[a-zA-Z]{2,4}$'", name = "chk_person_email"), nullable=False)
    gender   = mapped_column(String(20), nullable=False)
    ip_address  = mapped_column(String(15), nullable=False)

    # Defining 1-to-1 relationship between Person instance and Country instance
    country = relationship("Country", back_populates="person", uselist = False)

    def __init__(self, id, first_name, last_name, email, gender, ip_address):
        self.id = id
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.gender = gender
        self.ip_address = ip_address

    def to_dict(self):
        return {
            'id': self.id,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'email': self.email,
            'gender': self.gender,
            'ip_address': self.ip_address
        }

class Country(Base):
    __tablename__ = "country"

    id = mapped_column(Integer, autoincrement=True, primary_key=True)
    country  = mapped_column(String(2), nullable=False)
    person_id = mapped_column(Integer, ForeignKey("person.id"))

    # Defining 1-to-1 relationship between Country instance and Person instance
    person = relationship("Person", back_populates="country")

    def __init__(self, id, person_id, country):
        self.id = id
        self.person_id = person_id
        self.country = country