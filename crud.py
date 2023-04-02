import datetime
import csv
from helper import convert_to_UTC, if_unvalid, last_week_check,last_day_check, last_hour_check
from models import Storerecords , StoreWorkingHours , Timezones ,Status
from sqlalchemy import func
from sqlalchemy.orm import Session

def get_status(db:Session,report_ID):
    status = db.query(Status).filter(Status.report_id==report_ID).first()
    print(status.status)
    if status.status=="running":
        return False
    return True

def add_to_status(db:Session,report_ID):
    entry = Status(report_id=str(report_ID),status="running")
    db.add(entry)
    db.commit()
    db.refresh(entry)
   
def make_report(db: Session,report_id):
        Store_Records = db.query(Storerecords)
        Store_Working_Hours = db.query(StoreWorkingHours)
        timeZone = db.query(Timezones)
        store_working_hours = Store_Working_Hours.order_by(StoreWorkingHours.store_id).all()
        timezones = timeZone.order_by(Timezones.store_id).all()
        store_records = Store_Records.order_by(Storerecords.store_id).all()
        #Converting All time of closing and opening to UTC to have easier comparision later 
        Map_Of_IDDay_With_WorkingHours = convert_to_UTC(store_working_hours,timezones)
        starting_default = datetime.datetime(2023, 12, 25,hour=0,minute=0,second=0)
        Map_Of_answer_for_each_store = {}
        Total_Downtime_Uptime_Storewise = {}
        Map_Of_StoreID_With_Timestamps = {}
        # we were asked to set it max of our data
        # coded in a way that even for current time we just need to change line below
        current_time = datetime.datetime(2023, 1, 25, 18, 13, 22, 479220)
        # Code for the actual report generation starts here rest all was preprocessing
        for record in store_records:
            #CASE: If we have no data of store closing and opening then assuming it always open
            if record.store_id in Map_Of_StoreID_With_Timestamps:
                Map_Of_StoreID_With_Timestamps[record.store_id].append([record.status,record.timestamp])
            else:
                Map_Of_StoreID_With_Timestamps[record.store_id] = []
        
        #Here we go store by store for that store we go timestamp by timestamp 
        for store in Map_Of_StoreID_With_Timestamps:
            flag=0;
            Map_Of_answer_for_each_store[store] = [0,0,0,0,0,0]
            Total_Downtime_Uptime_Storewise[store] = [0,0]
            for Status_Time in Map_Of_StoreID_With_Timestamps[store]:
                # CASE:For the First Timestamp we need to have the starting time since we have no previous data
                if flag==0:
                    if last_hour_check(Status_Time[1],current_time):
                        if Status_Time[0]=="active":
                            if (store,Status_Time[1].weekday()) in Map_Of_IDDay_With_WorkingHours:
                                Map_Of_answer_for_each_store[store][0]+= min(((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][0]).seconds%3600)/60.0,60)
                            else:
                                Map_Of_answer_for_each_store[store][0]+= min(((Status_Time[1] - starting_default).seconds%3600)/60.0,60) 
                        else:
                            if (store,Status_Time[1].weekday()) in Map_Of_IDDay_With_WorkingHours:
                                Map_Of_answer_for_each_store[store][3]+= min(((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][1]).seconds%3600)/60.0,60) 
                            
                            else:
                                Map_Of_answer_for_each_store[store][3]+= min(((Status_Time[1] - starting_default).seconds%3600)/60.0,60)                               
                    if last_day_check(Status_Time[1],current_time):
                        if Status_Time[0]=="active":
                            if (store,Status_Time[1].weekday()) in Map_Of_IDDay_With_WorkingHours:
                                Map_Of_answer_for_each_store[store][1]+= ((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][0]).seconds)//3600
                            else:
                                Map_Of_answer_for_each_store[store][1]+= ((Status_Time[1] - starting_default).seconds%3600)/60.0
                        else:
                            if (store,Status_Time[1].weekday()) in Map_Of_IDDay_With_WorkingHours:
                                Map_Of_answer_for_each_store[store][4]+= ((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][1]).seconds)//3600
                            else:
                                Map_Of_answer_for_each_store[store][4]+= ((Status_Time[1] - starting_default).seconds)//3600
                    
                    if last_week_check(Status_Time[1],current_time):
                        if Status_Time[0]=="active":
                            if (store,Status_Time[1].weekday()) in Map_Of_IDDay_With_WorkingHours:
                                Map_Of_answer_for_each_store[store][2]+= ((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][0]).seconds)//3600
                            else:
                                Map_Of_answer_for_each_store[store][2]+= ((Status_Time[1] - starting_default).seconds)//3600
                        else:
                            if (store,Status_Time[1].weekday()) in Map_Of_IDDay_With_WorkingHours:
                                Map_Of_answer_for_each_store[store][5]+= ((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][1]).seconds)//3600
                            else:
                                Map_Of_answer_for_each_store[store][5]+= ((Status_Time[1] - starting_default).seconds)//3600
                    
                    if (store,Status_Time[1].weekday()) in Map_Of_IDDay_With_WorkingHours:
                        if Status_Time[0]=="active":
                            Total_Downtime_Uptime_Storewise[store][1]+= ((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][0]).seconds%3600)/60.0
                        else:
                            Total_Downtime_Uptime_Storewise[store][0]+= ((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][0]).seconds%3600)/60.0                    
                    else:
                        if Status_Time[0]=="active":
                            Total_Downtime_Uptime_Storewise[store][1]+= ((Status_Time[1] - starting_default).seconds%3600)/60.0
                        else:
                            Total_Downtime_Uptime_Storewise[store][0]+= ((Status_Time[1] - starting_default).seconds%3600)/60.0
                    flag=1;
                    prev = Status_Time
                else:
                    #if we have the same day then on switch of state i.e from active to inactive or vice-versa
                    #for switch we use the ratio of downtime with total time till now to get a extrapolation
                    #in case of no switch we assume between time we were in the same state
                    #The above observation makes sense with real life scenarios most of the time 
                    # we check the timestamp's validty according to the time and update 
                    if prev[1].date() == Status_Time[1].date():
                        if prev[0]==Status_Time[0]:
                            if Status_Time[0]=="active":
                                if last_hour_check(Status_Time[1],current_time):
                                    Map_Of_answer_for_each_store[store][0] += ((prev[1] - Status_Time[1]).seconds%3600)/60.0
                                if last_day_check(Status_Time[1],current_time):
                                    Map_Of_answer_for_each_store[store][1] += ((prev[1] - Status_Time[1]).seconds)//3600
                                if last_week_check(Status_Time[1],current_time):
                                    Map_Of_answer_for_each_store[store][2] += ((prev[1] - Status_Time[1]).seconds)//3600
                                Total_Downtime_Uptime_Storewise[store][1]+= ((prev[1] - Status_Time[1]).seconds%3600)/60.0    
                            else:
                                if last_hour_check(Status_Time[1],current_time):
                                    Map_Of_answer_for_each_store[store][3] += ((prev[1] - Status_Time[1]).seconds%3600)/60.0
                                if last_day_check(Status_Time[1],current_time):
                                    Map_Of_answer_for_each_store[store][4] += ((prev[1] - Status_Time[1]).seconds)//3600
                                if last_week_check(Status_Time[1],current_time):
                                    Map_Of_answer_for_each_store[store][5] += ((prev[1] - Status_Time[1]).seconds)//3600
                                Total_Downtime_Uptime_Storewise[store][0]+= ((prev[1] - Status_Time[1]).seconds%3600)/60.0    
                        else:
                            ratio = Total_Downtime_Uptime_Storewise[0]/(Total_Downtime_Uptime_Storewise[0]+Total_Downtime_Uptime_Storewise[1])
                            if last_hour_check(Status_Time[1],current_time):
                                Map_Of_answer_for_each_store[store][0] += (((prev[1] - Status_Time[1]).seconds%3600)/60.0)*(1-ratio)
                                Map_Of_answer_for_each_store[store][3] += (((prev[1] - Status_Time[1]).seconds%3600)/60.0)*(ratio)
                            if last_day_check(Status_Time[1],current_time):
                                Map_Of_answer_for_each_store[store][1] += ((prev[1] - Status_Time[1]).seconds)//3600*(1-ratio)
                                Map_Of_answer_for_each_store[store][4] += ((prev[1] - Status_Time[1]).seconds)//3600*(ratio)

                            if last_week_check(Status_Time[1],current_time):
                                Map_Of_answer_for_each_store[store][2] += ((prev[1] - Status_Time[1]).seconds)//3600*(1-ratio)
                                Map_Of_answer_for_each_store[store][5] += (((prev[1] - Status_Time[1]).seconds)//3600)*(ratio)
                            Total_Downtime_Uptime_Storewise[store][1]+= ((prev[1] - Status_Time[1]).seconds%3600)/60.0*(1-ratio)
                            Total_Downtime_Uptime_Storewise[store][0] += (((prev[1] - Status_Time[1]).seconds%3600)/60.0)*(ratio)
                                                              
                    else:
                        if last_hour_check(Status_Time[1],current_time):
                            if Status_Time[0]=="active":
                                if (store,Status_Time[1].weekday()) in Map_Of_IDDay_With_WorkingHours:
                                    Map_Of_answer_for_each_store[store][0]+= min(((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][0]).seconds%3600)/60.0,60)
                                else:
                                    Map_Of_answer_for_each_store[store][0]+= min(((Status_Time[1] - starting_default).seconds%3600)/60.0,60) 
                            else:
                                if (store,Status_Time[1].weekday()) in Map_Of_IDDay_With_WorkingHours:
                                    Map_Of_answer_for_each_store[store][3]+= min(((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][1]).seconds%3600)/60.0,60) 
                                
                                else:
                                    Map_Of_answer_for_each_store[store][3]+= min(((Status_Time[1] - starting_default).seconds%3600)/60.0,60)                               
                        if last_day_check(Status_Time[1],current_time):
                            if Status_Time[0]=="active":
                                if (store,Status_Time[1].weekday()) in Map_Of_IDDay_With_WorkingHours:
                                    Map_Of_answer_for_each_store[store][1]+= ((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][0]).seconds)//3600
                                else:
                                    Map_Of_answer_for_each_store[store][1]+= ((Status_Time[1] - starting_default).seconds)//3600
                            else:
                                if (store,Status_Time[1].weekday()) in Map_Of_IDDay_With_WorkingHours:
                                    Map_Of_answer_for_each_store[store][4]+= ((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][1]).seconds)//3600
                                else:
                                    Map_Of_answer_for_each_store[store][4]+= ((Status_Time[1] - starting_default).seconds)//3600
                        
                        if last_week_check(Status_Time[1],current_time):
                            if Status_Time[0]=="active":
                                if (store,Status_Time[1].weekday()) in Map_Of_IDDay_With_WorkingHours:
                                    Map_Of_answer_for_each_store[store][2]+= ((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][0]).seconds)//3600
                                else:
                                    Map_Of_answer_for_each_store[store][2]+= ((Status_Time[1] - starting_default).seconds)//3600
                            else:
                                if (store,Status_Time[1].weekday()) in Map_Of_IDDay_With_WorkingHours:
                                    Map_Of_answer_for_each_store[store][5]+= ((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][1]).seconds)//3600
                                else:
                                    Map_Of_answer_for_each_store[store][5]+= ((Status_Time[1] - starting_default).seconds%3600)//3600
                                    
                        if (store,Status_Time[1].weekday()) in Map_Of_IDDay_With_WorkingHours:
                            if Status_Time[0]=="active":
                                Total_Downtime_Uptime_Storewise[store][1]+= ((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][0]).seconds%3600)/60.0
                            else:
                                Total_Downtime_Uptime_Storewise[store][0]+= ((Status_Time[1] - Map_Of_IDDay_With_WorkingHours[(store,Status_Time[1].weekday())][0]).seconds%3600)/60.0                    
                        else:
                            if Status_Time[0]=="active":
                                Total_Downtime_Uptime_Storewise[store][1]+= ((Status_Time[1] - starting_default).seconds%3600)/60.0
                            else:
                                Total_Downtime_Uptime_Storewise[store][0]+= ((Status_Time[1] - starting_default).seconds%3600)/60.0
                prev = Status_Time
                Map_Of_answer_for_each_store[store][0] = min(Map_Of_answer_for_each_store[store][0],60)
                Map_Of_answer_for_each_store[store][1] = min(Map_Of_answer_for_each_store[store][1],60)
        
        file = open(str(report_id)+'.csv', 'w', newline ='')
        with file:
            header = ['store_id','uptime_last_hour(in minutes)','uptime_last_day(in hours)',
                      'uptime_last_week(in hours)','downtime_last_hour(in minutes)','downtime_last_day(in hours)'
                      ,'downtime_last_week(in hours)']
            writer = csv.DictWriter(file, fieldnames = header)
            writer.writeheader()
            for key in Map_Of_answer_for_each_store:
                answer_entry = {}
                answer_entry['store_id'] = key
                answer_entry['uptime_last_hour(in minutes)']=Map_Of_answer_for_each_store[key][0]
                answer_entry['uptime_last_day(in hours)']=Map_Of_answer_for_each_store[key][1]
                answer_entry['uptime_last_week(in hours)']=Map_Of_answer_for_each_store[key][2]
                answer_entry['downtime_last_hour(in minutes)']=Map_Of_answer_for_each_store[key][3]
                answer_entry['downtime_last_day(in hours)']=Map_Of_answer_for_each_store[key][4]
                answer_entry['downtime_last_week(in hours)']=Map_Of_answer_for_each_store[key][5]
                writer.writerow(answer_entry)     
        
        #To update that we're done generating report 
        db.query(Status).filter(report_id==report_id).update({'status':'Done'})
        db.commit()

        
        
                
        
            
                
                
                    
            
        
        
                