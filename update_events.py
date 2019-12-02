import boto3
from boto3.dynamodb.conditions import Key
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

    #user_id = 'Dolphinsphan13'
    #password = 'Edwards68'
    
    user_id = os.environ['user_id']
    password = os.environ['password']
    
    horses_table = client.Table('horses')
    resutls_table = client.Table('results')
    entries_table = client.Table('entries')
    workouts_table = client.Table('workouts')

    #base_path = os.environ['base_path']
    base_path = 'https://data.tjcis.com/api/portfolio'
    
    results = None
    entries = None
    workouts = None
    result_records = False
    entry_records = False
    workout_records = False
    todays_datetime = datetime.now(tz=pytz.timezone('US/Eastern'))
    
    
    horses = horses_table.scan()
    for each in horses['Items']:
        unique_id = each['unique_id']
        horse_name = each['horse_name']
        
        results_call = f"base_path/{unique_id}/race/results"
        
        #print('results_call - ' + results_call)
        
        try:
            response = requests.get(results_call, auth=(user_id, password))
            
            #print(response)
            
            results = response.json()
            result_records = True
        except:
            result_records = False
        
        if result_records:
            for i in range(len(results)):
                event_date = datetime_from_string(results[i]['raceDate'], '6:00')

                #print(todays_datetime)
                #print(event_date)
                
                if(todays_datetime - event_date < timedelta(days=8)):
                    result = results[i]
                    item = build_result(result)
                    resutls_table.put_item(Item=item)
                
                    entries_table.delete_item(
                    Key={
                        'unique_id': item['unique_id'],
                        'Entry_Date': item['Event_Date']
                    })
                result_records = False
            
        entries_call = f"base_path/{unique_id}/race/entries"
                
        #print(f"entries_call - {entries_call}")
        
        try:
            #print(horse_name)
            response = requests.get(entries_call, auth=(user_id, password))
            
            #print(response)
            #print('with entries')
            #print('response reason ' + response['reason'])
            
            entries = response.json()
            entry_records = True
            
        except Exception as error:
            #print(error.__str__())
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
                
                existing_entry = entries_table.query(KeyConditionExpression=Key('Entry_Date').eq(entry_date) & Key('unique_id').eq(unique_id))
                
                #print(existing_entry)
                entry_datetime = datetime_from_string(entry_date, post_time)
                
                #print(todays_datetime)
                #print(entry_datetime)

                if (existing_entry['Count'] == 0) and  (entry_datetime > todays_datetime):
                    item = build_entry(entries)
                    entries_table.put_item(Item=item)
                    
                    #print('entry added')
            
        elif entry_records and type(entries) == dict:
            entry_date = entries['raceDate']
            unique_id = entries['uniqueId']
            post_time = entries['postTime']
            
            #print(entry_date)
            #print(unique_id)
            
            existing_entry = entries_table.query(KeyConditionExpression=Key('Entry_Date').eq(entry_date) & Key('unique_id').eq(unique_id))
            
            entry_datetime = datetime_from_string(entry_date, post_time)
            if (existing_entry['Count'] == 0) and (entry_datetime > todays_datetime):
                item = build_entry(entries)
                entries_table.put_item(Item=item)   
                
                #print('entry added')
        
        entry_records = False

        workouts_call = f"base_path/{unique_id}/workouts"
        
        #print(f"workouts_call - {workouts_call}")
        
        try:
            response = requests.get(workouts_call, auth=(user_id, password))
            
            #print(response)
            #print('response reason ' + response['reason'])
            
            workouts = response.json()
            workout_records = True
        except:
            workout_records = False
            
        #print(f"workouts type - {type(workouts)}")
        
        if workout_records and type(workouts) == dict:
            event_date = datetime_from_string(workouts['raceDate'], '6:00')
            if(todays_datetime - event_date < timedelta(days=15)):
                item = build_workout(workouts)
                workouts_table.put_item(Item=item)
        elif workout_records and type(workouts) == list and len(workouts) > 0:
            for a in range(len(workouts)):
                event_date = date_from_string(workouts[a]['workoutDate'])
                min_date = get_min_date()
                if(event_date > min_date):
                    workout = workouts[a]
                    item = build_workout(workout)
                    workouts_table.put_item(Item=item)            
            workout_records = False        

    return True

#lambda_handler('', '')