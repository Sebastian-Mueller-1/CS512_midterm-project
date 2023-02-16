import requests
import json
import csv
import pandas as pd
import re
import tarfile
import sqlite3
from pathlib import Path
import re
from statistics import mean
import copy
import matplotlib.pyplot as plt

# open and extract data from compressed yelp tzg file
tar_file = tarfile.open('C:\\Users\\sebmu\\Desktop\\CS512\\CS512_WRANGLE\\yelp_dataset.tgz')
tar_file.extractall('./midterm/')
tar_file.close()

# each JSON was file of individual JSON objects, could not load effectively. needed to make listed, open JSON file, and for each object load it and append it into dictionary. Leaves the 5 lists closer to normal JSON formating, just missing global key.

review_list = []
business_list = []
checkin_list = []
tip_list = []
user_list = []

with open("C:\\Users\\sebmu\\Desktop\\CS512\\CS512_WRANGLE\\midterm\\yelp_academic_dataset_review.json") as j:
    for json_object in j:
        review_dict = json.loads(json_object)
        review_list.append(review_dict)

with open("C:\\Users\\sebmu\\Desktop\\CS512\\CS512_WRANGLE\\midterm\\yelp_academic_dataset_business.json") as j:
    for json_object in j:
        business_dict = json.loads(json_object)
        business_list.append(business_dict)

with open("C:\\Users\\sebmu\\Desktop\\CS512\\CS512_WRANGLE\\midterm\\yelp_academic_dataset_checkin.json") as j:
    for json_object in j:
        checkin_dict = json.loads(json_object)
        checkin_list.append(checkin_dict)

with open("C:\\Users\\sebmu\\Desktop\\CS512\\CS512_WRANGLE\\midterm\\yelp_academic_dataset_tip.json") as j:
    for json_object in j:
        tip_dict = json.loads(json_object)
        tip_list.append(tip_dict)

with open("C:\\Users\\sebmu\\Desktop\\CS512\\CS512_WRANGLE\\midterm\\yelp_academic_dataset_user.json") as j:
    for json_object in j:
        user_dict = json.loads(json_object)
        user_list.append(user_dict)

# convert 5 lists to pd df, then to CSV. Converting to CSV makes loading into SQL DB easier. 
dfreview = pd.DataFrame(review_list)
dfreview.to_csv('reviews.csv',index=False, na_rep="Null")

dfbusiness = pd.DataFrame(business_list)
dfbusiness.to_csv('businessInfo.csv',index=False, na_rep="Null")

dfcheckin = pd.DataFrame(checkin_list)
dfcheckin.to_csv('checkin.csv',index=False, na_rep="Null")

dftip = pd.DataFrame(tip_list)
dftip.to_csv('tip.csv',index=False, na_rep="Null")

dfuser = pd.DataFrame(user_list)
dfuser.to_csv('user.csv',index=False, na_rep="Null")

#initialize SQLite DB and cursor object for project
connection = sqlite3.connect("C:\sqlite\CS512_midterm_project_test50.sqlite")
c = connection.cursor()

# create business table and populate from business CSV
c.execute("""CREATE TABLE businessInfo(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id TEXT,
    name TEXT,
    address TEXT, 
    city TEXT,
    state TEXT,
    postal_code TEXT,
    latitude FLOAT,
    longitude FLOAT, 
    stars FLOAT,
    review_count INTEGER, 
    is_open INTEGER, 
    attributes TEXT,
    categories TEXT,
    hours TEXT
);""")

# create reviews table and populate from review CSV
c.execute("""CREATE TABLE review(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id TEXT,
    user_id TEXT,
    business_id TEXT,
    stars FLOAT,
    useful FLOAT,
    funny FLOAT,
    cool FLOAT,
    text TEXT,
    date TEXT
);""")

# create checkin table and populate from checkin CSV
c.execute("""CREATE TABLE checkIn(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id TEXT, 
    date TEXT
);""")

# create checkin table and populate from tip CSV
c.execute("""CREATE TABLE tip(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT, 
    business_id TEXT, 
    text TEXT,
    date TEXT,
    compliment_count FLOAT
);""")

# create checkin table and populate from users CSV
c.execute("""CREATE TABLE users(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT, 
    name TEXT, 
    review_count INTEGER,
    yelping_since TEXT, 
    useful INTEGER, 
    funny INTEGER, 
    cool INTEGER, 
    elite TEXT, 
    friends TEXT,
    fans INTEGER,
    average_stars FLOAT,
    compliment_hot INTEGER,
    compliment_more INTEGER,
    compliment_profile INTEGER,
    compliment_cute INTEGER,
    compliment_list INTEGER,
    compliment_note INTEGER,
    compliment_plain INTEGER,
    compliment_cool INTEGER,
    compliment_funny INTEGER,
    compliment_writer INTEGER,
    compliment_photos INTEGER
);""")

# convert to pd df, input to SQL DB
checkin_info = pd.read_csv('checkin.csv')
checkin_info.to_sql('checkIn', connection, if_exists='append', index=False)

# convert to pd df, input to SQL DB
user_info = pd.read_csv('user.csv')
user_info.to_sql('user', connection, if_exists='append', index=False)

# convert to pd df, input to SQL DB
tip_info = pd.read_csv('tip.csv')
tip_info.to_sql('tip', connection, if_exists='append', index=False)

# convert to pd df, input to SQL DB
business_info = pd.read_csv('businessInfo.csv')
business_info.to_sql('businessInfo', connection, if_exists='append', index=False)

# convert to pd df, input to SQL DB
review_info = pd.read_csv('reviews.csv')
review_info.to_sql('review', connection, if_exists='append', index=False)

# -------------------3 question exploration-----------------------------

# question 1
clean_list = []
dirty_list = []
clean_dirty_list = []

for i in range(0, len(review_info)): # loop takes ~30 min to execute
    temp = re.search("clean.*dirt|dirt.*clean", review_info.iloc[i]['text'])
    if temp != None:
        clean_dirty_list.append(review_info.iloc[i]['stars'])
        continue

    temp = re.search("clean", review_info.iloc[i]['text'])
    if temp != None:
        clean_list.append(review_info.iloc[i]['stars'])
        continue

    temp = re.search("dirt", review_info.iloc[i]['text'])
    if temp != None:
        dirty_list.append(review_info.iloc[i]['stars'])

avg_clean = mean(clean_list)
avg_dirty = mean(dirty_list)
avg_clean_dirty_list = mean(clean_dirty_list)

# question 2
bitcoin_list_rating = []
non_bitcoin_list_rating = []

for i in range(0, len(business_info)):
    temp = re.search("'BusinessAcceptsBitcoin': 'True'", business_info.iloc[i]['attributes'])
    if temp != None:
        bitcoin_list_rating.append(business_info.iloc[i]['stars'])
    else:
        non_bitcoin_list_rating.append(business_info.iloc[i]['stars'])

# question 3
states = {
    'AB': [],
    'AK': [],
    'AL': [],
    'AR': [],
    'AZ': [],
    'CA': [],
    'CO': [],
    'CT': [],
    'DC': [],
    'DE': [],
    'FL': [],
    'GA': [],
    'HI': [],
    'IA': [],
    'ID': [],
    'IL': [],
    'IN': [],
    'KS': [],
    'KY': [],
    'LA': [],
    'MA': [],
    'MD': [],
    'ME': [],
    'MI': [],
    'MN': [],
    'MO': [],
    'MS': [],
    'MT': [],
    'NC': [],
    'ND': [],
    'NE': [],
    'NH': [],
    'NJ': [],
    'NM': [],
    'NV': [],
    'NY': [],
    'OH': [],
    'OK': [],
    'OR': [],
    'PA': [],
    'RI': [],
    'SC': [],
    'SD': [],
    'TN': [],
    'TX': [],
    'UT': [],
    'VA': [],
    'VT': [],
    'WA': [],
    'WI': [],
    'WV': [],
    'WY': [],
    'XMS': [],
    'VI': []
}

for i in range(0, len(business_info)):
    states[business_info.iloc[i]['state']].append(business_info.iloc[i]['stars'])

temp = []
for key, value in states.items():
    if len(value) == 0:
        temp.append(key)

for i in temp:
    del states[i]

states_deepcopy = copy.deepcopy(states)

for key, value in states.items():
    states_deepcopy[key] = mean(states[key])

# 3 plots
#q1
catagories = ["clean", "dirt or dirty", "clean and dirt/dirty"]
vals = [avg_clean, avg_dirty, avg_clean_dirty_list]

fig, ax= plt.subplots()
plt.bar(catagories,vals)
plt.ylabel("Average Star Rating")
plt.show()

#q2
states_list = []
states_values = []
for key, value in states_deepcopy.items():
    states_list.append(key)
    states_values.append(value)

fig = plt.figure(figsize=(12,6))
plt.bar(states_list ,states_values, width=.4)
plt.ylabel("Average Star Rating")
plt.show()

#q3
vals = [mean(bitcoin_list_rating),mean(non_bitcoin_list_rating)]

fig = plt.figure()
plt.bar(['accepts bitcoin', 'does not accept bitcoin'],vals)
plt.ylabel("Average Star Rating")
