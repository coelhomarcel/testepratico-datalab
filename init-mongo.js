db = db.getSiblingDB('airflow')

db.createUser({
    user: 'airflow',
    pwd: 'airflow',
    roles: [
      {
        role: 'readWrite',
        db: 'airflow'
      }
    ]
})

db.createCollection('variables');
