from pymongo import MongoClient

CONNECTION_STRING = "mongodb://airflow:airflow@mongo:27017/airflow"

def get_criteria(DAG_ID):
    mongo = MongoClient(CONNECTION_STRING)
    airflow_vars = mongo['airflow']['variables']
    dag_vars = airflow_vars.find_one({"DAG_ID" : DAG_ID})
    return dag_vars

def insert_criteria(DAG_ID, date):
    mongo = MongoClient(CONNECTION_STRING)
    airflow_vars = mongo['airflow']['variables']
    airflow_vars.insert_one({
        "DAG_ID": DAG_ID,
        "date": date
    })

def update_criteria(DAG_ID, date):
    mongo = MongoClient(CONNECTION_STRING)
    airflow_vars = mongo['airflow']['variables']
    airflow_vars.update_one({"DAG_ID": DAG_ID},{
        "$set": {
            "date": date
        }
    })