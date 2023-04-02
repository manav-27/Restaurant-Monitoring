from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session
import threading
import crud
import models
from database import SessionLocal, engine
import uuid
import pandas
models.Base.metadata.create_all(bind=engine)

app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/trigger_report")
async def trigger_report(db: Session = Depends(get_db)):
    # Report ID generated using uuid so report_id's are as unique as possible
    report_id = uuid.uuid4()
    # We add the current report ID to running status in Database
    crud.add_to_status(db,report_id)
    # We need Threads because this function will return report_id
    # and the thread will continue to generate the report
    t1 = threading.Thread(target=crud.make_report, args=(db,report_id))
    t1.start()
    return {"Report_ID":report_id}


@app.get("/get_report/{report_id}")
async def get_report(report_id : str,db: Session = Depends(get_db)):
    #If done then return Done :)
    # We store CSV file here only we can make it downloadable by just another API Call
    # here with one call downloading and giving done response didn't seem to work together 
    if crud.get_status(db,report_id):
        CSV = pandas.read_csv(report_id+".csv")
        Output = {"Status":"Complete"}
        Output.update(CSV.to_dict())
        return Output
    return {"Status":"Running"}
    