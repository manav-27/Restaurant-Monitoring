from sqlalchemy import Boolean, Column, ForeignKey, Integer, String , DateTime ,Time
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import VARCHAR
from database import Base
class Storerecords(Base):
    __tablename__ = "Storerecords"

    store_id = Column(String, primary_key=True, index=True)
    status = Column(String)
    timestamp = Column(DateTime,primary_key=True)

class StoreWorkingHours(Base):
    __tablename__ = "StoreWorkingHours"
    
    store_id = Column(String, primary_key=True, index=True)
    day = Column(Integer,primary_key=True, index=True) # add range 0-6
    start_time = Column(Time,primary_key=True)
    end_time = Column(Time)    

class Timezones(Base):
    __tablename__ = "Timezones"
    store_id = Column(String, primary_key=True, index=True)
    timezone = Column(String,default="America/Chicago")
    
class Status(Base):
    __tablename__ = "Status"
    report_id = Column(String,primary_key=True)
    status = Column(String,default="Running")
    
    


