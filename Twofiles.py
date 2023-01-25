from flask import Flask, render_template, request, send_file
import pandas as pd
import os
from pymongo import MongoClient

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/', methods=['POST'])
def update_db():
    client = MongoClient("mongodb+srv://nexuses:nexuses123@cluster0.pbrqpu9.mongodb.net/?retryWrites=true&w=majority")
    db = client["Nex-Data"]
    main_db_collection = db["Data"]
    main_db = pd.DataFrame.from_records(main_db_collection.find())

    secondary_db_file = request.files['secondary_db_file']
    print(main_db)
    secondary_db = pd.read_csv(secondary_db_file)
    print(secondary_db)
    main_db = main_db.rename(columns={'first_name': 'first_name', 'last_name': 'last_name','email': 'email','company':'company'})
    secondary_db = secondary_db.rename(columns={'first_name': 'first_name', 'last_name': 'last_name','email': 'email','company':'company'})
   
    main_db = main_db.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
    secondary_db = secondary_db.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
    
    updated_db = pd.merge(secondary_db, main_db, on=['first_name', 'last_name', 'company'], how='left', suffixes=('', '_y'))
    updated_db['email'] = updated_db['email'].combine_first(updated_db['email_y'])
    updated_db = updated_db.drop(['email_y'], axis=1)
    updated_db.to_csv('updated_db.csv', index=False)
    
    # Return the updated_db.csv file as a download
    return send_file('updated_db.csv', as_attachment=True)

if __name__ == '__main__':
    app.run(host='45.76.227.41', port=8502, debug=True)