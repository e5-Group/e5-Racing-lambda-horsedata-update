import boto3
from boto3.dynamodb.conditions import Key, Attr
import requests
import os
from datetime import datetime, date, timedelta
from dateutil import parser
from pytz import timezone
import pytz
from utils import build_entry, build_result, build_workout, get_min_date, date_from_string, datetime_from_string

def lambda_handler(event, context):
    client = boto3.resource('dynamodb')

    #client = boto3.resource(
    #'dynamodb', 
    #aws_access_key_id = 'AKIAIAPYES55LNB4XHWQ',
    #aws_secret_access_key = '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',
    #region_name= 'us-east-1')

    horses_table = client.Table('horses')


    todays_datetime = datetime.now(tz=pytz.timezone('US/Eastern'))

    base_path = os.environ['base_path']
    #base_path = 'https://data.tjcis.com/api/portfolio'
    
    portfolios = get_portfolios(client)

    for portfolio in portfolios['Items']:
    
        user_id = portfolio['user']
        password = portfolio['password']
        brand = portfolio['brand']

        if brand == 'e5':
            resutls_table = client.Table('results')
            entries_table = client.Table('entries')
            workouts_table = client.Table('workouts')
        elif brand == 'MC':
            resutls_table = client.Table('results_mc')
            entries_table = client.Table('entries_mc')
            workouts_table = client.Table('workouts_mc')


        horses = horses_table.scan(FilterExpression=Attr('brand').eq(brand))
        for each in horses['Items']:
            unique_id = each['unique_id']
            horse_name = each['horse_name']
            
            print('in horses *****************')
            print(brand + ':' + horse_name)

            process_results(brand, user_id, password, base_path, unique_id, todays_datetime, resutls_table, entries_table)
                
            process_entries(brand, user_id, password, base_path, unique_id, todays_datetime, entries_table)

            process_workouts(brand, user_id, password, base_path, unique_id, todays_datetime, workouts_table)

        brand = None
        user_id = None
        password = None

    return True

def get_portfolios(database):
    portfolio_table = database.Table('portfolios')
    
    portfolios = portfolio_table.scan()

    return portfolios  

def process_results(brand, user_id, password, base_path, unique_id, todays_datetime, resutls_table, entries_table):
    results = None
    result_records = False

    results_call = base_path + '/' + unique_id + '/race/results'
    
    print('results_call - ' + results_call)
    
    try:
        response = requests.get(results_call, auth=(user_id, password))
        
        print('in results *****************')
        
        #print(response)
        
        results = response.json()
        result_records = True
    except:
        result_records = False

    if result_records:
        for i in range(len(results)):
            event_date = datetime_from_string(results[i]['raceDate'], '6:00')
            
            print(todays_datetime)
            print(event_date)
            
            if(todays_datetime - event_date < timedelta(days=8)):
                result = results[i]
                item = build_result(result, brand)
                resutls_table.put_item(Item=item)
            
                entries_table.delete_item(
                Key={
                    'unique_id': item['unique_id'],
                    'Entry_Date': item['Event_Date']
                })
            result_records = False
        
def process_entries(brand, user_id, password, base_path, unique_id, todays_datetime, entries_table):
    entries = None
    entry_records = False

    entries_call = base_path + '/' + unique_id + '/race/entries'
            
    #print(f"entries_call - {entries_call}")
    
    try:
        response = requests.get(entries_call, auth=(user_id, password))
        
        print('in entries *****************')
        
        #print(response)
        #print('with entries')
        #print('response reason ' + response['reason'])
        
        entries = response.json()
        entry_records = True
        
    except Exception as error:
        print(error.__str__())
        print(error)
        #print(response)
        #print('with no entries')
        entry_records = False
        
    #print(f"entries type - {type(entries)}")
    #print(entry_records)

    if entry_records and type(entries) == list and len(entries) > 0:
        for i in range(len(entries)):
            entry = entries[i]
            entry_date = entry['raceDate']
            unique_id = entry['uniqueId']
            post_time = entry['postTime']
            
            print(todays_datetime)
            print(entry_date)
            
            existing_entry = entries_table.query(KeyConditionExpression=Key('Entry_Date').eq(entry_date) & Key('unique_id').eq(unique_id))
            
            #print(existing_entry)
            entry_datetime = datetime_from_string(entry_date, post_time)

            if (existing_entry['Count'] == 0) and  (entry_datetime > todays_datetime):
                item = build_entry(entries, brand)
                entries_table.put_item(Item=item)
                
                #print('entry added')
        
    elif entry_records and type(entries) == dict:
        entry_date = entries['raceDate']
        unique_id = entries['uniqueId']
        post_time = entries['postTime']
        
        #print(entry_date)
        #print(unique_id)
        
        print(todays_datetime)
        print(entry_date)
        
        existing_entry = entries_table.query(KeyConditionExpression=Key('Entry_Date').eq(entry_date) & Key('unique_id').eq(unique_id))
        
        entry_datetime = datetime_from_string(entry_date, post_time)
        if (existing_entry['Count'] == 0) and (entry_datetime > todays_datetime):
            item = build_entry(entries, brand)
            entries_table.put_item(Item=item)   
            
            #print('entry added')

    entry_records = False

def process_workouts(brand, user_id, password, base_path, unique_id, todays_datetime, workouts_table):
    workouts = None
    workout_records = False

    workouts_call = base_path + '/' + unique_id  + '/workouts'
    
    #print(f"workouts_call - {workouts_call}")
    
    try:
        response = requests.get(workouts_call, auth=(user_id, password))
        
        print('in workouts *****************')
        
        #print(response)
        #print('response reason ' + response['reason'])
        
        workouts = response.json()
        workout_records = True
    except:
        workout_records = False
        
    #print(f"workouts type - {type(workouts)}")

    if workout_records and type(workouts) == dict:
        event_date = datetime_from_string(workouts['workoutDate'], '6:00')
        
        print(todays_datetime)
        print(event_date)
        
        if(todays_datetime - event_date < timedelta(days=15)):
            item = build_workout(workouts, brand)
            workouts_table.put_item(Item=item)
    elif workout_records and type(workouts) == list and len(workouts) > 0:
        for a in range(len(workouts)):
            event_date = datetime_from_string(workouts[a]['workoutDate'], '6:00')
            
            print(todays_datetime)
            print(event_date)
        
            if(todays_datetime - event_date < timedelta(days=15)):
                workout = workouts[a]
                item = build_workout(workout, brand)
                workouts_table.put_item(Item=item)            
        workout_records = False     

#lambda_handler('', '')