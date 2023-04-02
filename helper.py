from sqlalchemy.orm import Session
import pytz
import datetime
from datetime import timedelta
from models import Storerecords , StoreWorkingHours , Timezones
def convert_to_UTC(store_working_hours : list[StoreWorkingHours],timezones : list[Timezones]):
    Map_Of_IDDay_With_WorkingHours = {}
    count = 0
    for stores in store_working_hours:
        flag = 0
        for time in timezones:
            if(stores.store_id==time.store_id):
                flag=1
                opening_in_UTC = convert_one(time.timezone,stores.start_time)
                closing_in_UTC = convert_one(time.timezone,stores.end_time)
                Map_Of_IDDay_With_WorkingHours[(stores.store_id,stores.day)] = [opening_in_UTC,closing_in_UTC]
                
        if flag ==0:
            opening_in_UTC = convert_one("America/Chicago",stores.start_time)
            closing_in_UTC = convert_one("America/Chicago",stores.end_time)
            Map_Of_IDDay_With_WorkingHours[(stores.store_id,stores.day)] = [opening_in_UTC,closing_in_UTC]          
    return Map_Of_IDDay_With_WorkingHours

def if_unvalid(record :Storerecords,Map_Of_IDDay_With_WorkingHours):
    current_day = record.timestamp.weekday()
    current_store = record.store_id
    if (current_store,current_day) in Map_Of_IDDay_With_WorkingHours:
        if Map_Of_IDDay_With_WorkingHours[(current_store,current_day)][0]>record.timestamp.datetime.time() or Map_Of_IDDay_With_WorkingHours[[current_store,current_day]][1]<record.timestamp:
            return True
    
    return False

def last_week_check(timestamp : datetime,current_time : datetime):
    if(current_time - timestamp <=timedelta(days=7)):
        return True
    return False;
    
def last_day_check(timestamp : datetime,current_time : datetime):
    if(current_time - timestamp <=timedelta(days=1)):
        return True
    return False;

def last_hour_check(timestamp : datetime,current_time : datetime):
    if(current_time - timestamp <=timedelta(hours=1)):
        return True
    return False;


def convert_one(timezone : str,start_time : datetime.time):
    given_timezone = pytz.timezone(timezone)
    new_start_time = datetime.datetime(1000,11,10,start_time.hour,start_time.minute,start_time.second)
    local_datetime = given_timezone.localize(new_start_time, is_dst=None)
    Converted = local_datetime.astimezone(pytz.utc)
    return Converted