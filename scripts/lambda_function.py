import json
import boto3
import requests
import re

def clean_json_data(data):

    cleaned_data = []

    ## CLEANING THE JSON
    for index, entry in enumerate(data):
        # Add record_id
        entry['record_id'] = index
        
        # rename columns
        entry['age'] = entry.pop('vict_age', None)
        entry['sex'] = entry.pop('vict_sex', None)
        entry['descent'] = entry.pop('vict_descent', None)

        # group the crm_cd's
        crm_codes = []
        for i in range(2, 5):
            crm_codes.append(entry.pop(f'crm_cd_{i}', ''))
        crm_codes = ', '.join(filter(None, crm_codes))
        entry['other_crm_cds'] = crm_codes

        # positive victim age
        if entry['age'] is not None:
            entry['age'] = max(int(entry['age']), 0)

        # 24-hour format
        time_occ = entry.get('time_occ', '')
        if time_occ != '':
            if len(time_occ) == 3:
                time_occ = f"0{time_occ[0]}{time_occ[1]}:{time_occ[2]}"
            else:
                time_occ = f"{time_occ[:2]}:{time_occ[2:]}"
            entry['time_occ'] = time_occ

        # Remove unwanted spaces from location
        entry['location'] = re.sub(r'\s+', ' ', entry.get('location', ''))
        
        cleaned_data.append(entry)

    return cleaned_data

def lambda_handler(event, context):
    # Make API request to fetch JSON data
    api_url = 'https://data.lacity.org/resource/2nrs-mtv8.json'
    response = requests.get(api_url)
    
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': json.dumps('Failed to fetch data from API')
        }
    
    # Extract JSON data from API response
    input_data = response.json()
    
    # Clean the JSON data
    cleaned_data = clean_json_data(input_data)
    
    # Upload cleaned data to S3 (Input bucket)
    s3 = boto3.client('s3')
    bucket_name = ''
    key = 'crime_data.json'
    
    # Convert cleaned data to JSON string
    cleaned_data_json = json.dumps(cleaned_data)
    
    # Upload to S3
    s3.put_object(Bucket=bucket_name, Key=key, Body=cleaned_data_json)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data uploaded to S3 successfully')
    }
