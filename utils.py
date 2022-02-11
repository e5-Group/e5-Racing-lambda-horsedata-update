import datetime
from datetime import date, timedelta, datetime
from pytz import timezone
from dateutil import parser
import os

def format_date(date):
    formatted_date = date[:4] + '-' + date[4:6] + '-' + date[6:]
    return formatted_date
    
def date_from_string(input):
    if('-' not in input):
        temp_string = input[:4] + '-' + input[4:6] + '-' + input[6:]
    else:
        temp_string = input
        
    y2, m2, d2 = [int(x) for x in temp_string.split('-')]
    temp_date = date(y2, m2, d2)
    return temp_date
    
def datetime_from_string(input, input1):
    eastern = timezone('US/Eastern')
    ampm = ''
    if(input1.split(':')[0] in ('10', '11')): 
        ampm = ' AM' 
    else: 
        ampm = ' PM'

    if('-' not in input):
        temp_string = input[:4] + '-' + input[4:6] + '-' + input[6:] + ' ' + input1 + ampm
    else:
        temp_string = input + ' ' + input1 + ampm
        
    temp_datetime_notz = parser.parse(temp_string)
    temp_datetime = eastern.localize(temp_datetime_notz)
    return temp_datetime
    
def get_min_date():
    min_date = date.today() - timedelta(days=7)
    return min_date

def build_entry(entry, brand):
    item = {
        'Brand': brand,
        'unique_id': entry['uniqueId'],
        #'horse_name': entry['horseName'],
        'Horse_Name': entry['horseName'],
        #'race_date': entry['raceDate'],
        'Entry_Date': entry['raceDate'],
        #'track_code': entry['trackCode'],
        'Track': entry['trackCode'],
        #'race_number': entry['raceNumber'].__str__(),
        'Number_Entered': entry['raceNumber'].__str__(),
        'post_time': entry['postTime'],
        'time_zone': entry['timeZone'],
        'purse_amount': entry['purseAmount'].__str__(),
        'race_distance': entry['raceDistance'],
        #'race_type': entry['raceType'],
        'Class': entry['raceType'],
        #'race_name': entry['raceName'],
        'jockey_name': entry['jockeyName'],
        'trainer_name': entry['trainerName'],
        'Created_at': datetime.now().strftime('%Y-%m-%dT%H:%m'),
        'inital_notification': 'n',
        'daybefore_notification': 'n',
        'withinhour_notification': 'n',
        'fifteenmin_notification': 'n'
        }
        
    return item
    
def build_result(result, brand):
    item = {
            'Brand': brand,
            'unique_id': result["uniqueId"],
            #'horse_name': result["horseName"],
            'Horse_Name': result["horseName"],
            #'race_date': result["raceDate"],
            'Event_Date': format_date(result["raceDate"]),
            #'track_code': result["trackCode"],
            'Track': result["trackCode"],
            #'race_number': result["raceNumber"].__str__(),
            'Number_Entered': result["raceNumber"].__str__(),
            'track_condition': result["trackCondition"],
            'race_distance': result["raceDistance"],
            #'race_type': result["raceType"],
            'Class': result["raceType"],
            'race_value': result["raceValue"],
            #'race_name': result["raceName"],
            #'finish_position': result["finishPostion"].__str__(),
            'Finish': result["finishPostion"].__str__(),
            'final_time': result["finalTime"],
            'jockey_name': result["jockeyName"],
            'earnings': result["earnings"].__str__(),
            'chart_link': result["chartLink"],
            'Created_at': datetime.now().strftime('%Y-%m-%dT%H:%m')
            }    
    
    return item
    
def build_workout(workout, brand):
    if workout['horseName'] == '': 
        horse_name = 'Unnamed Horse'  
    else:
        horse_name = workout['horseName']

    item = {
            'Brand': brand,
            'unique_id': workout['uniqueId'],
            #'horse_name': workout['horseName'],
            'Horse_Name': horse_name,
            #'track_code': workout['trackCode'],
            'Track': workout['trackCode'],
            #'workout_date': workout['workoutDate'],
            'Event_Date': workout['workoutDate'],
            #'workout_distance': workout['workoutDistance'],
            'Distance': workout['workoutDistance'],
            'track_condition': workout['trackCondition'],
            #'workout_time': workout['workoutTime'],
            'Time': workout['workoutTime'],
            'workout_type': workout['workoutType'],
            'ranking': workout['ranking'],
            'Created_at': datetime.now().strftime('%Y-%m-%dT%H:%m')
            }
    print(item)
            
    return item
