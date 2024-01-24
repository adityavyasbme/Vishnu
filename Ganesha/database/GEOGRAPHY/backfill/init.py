import json
import psycopg2
import csv
# import urllib.request as urllib2
import requests
import pandas as pd 
import re
from tqdm import tqdm
import numpy as np
import zipfile
import io
import os
import shutil 

# Database connection parameters
db_params = {
    'dbname': 'DEV_CORE',
    'user': 'postgres',
    'password': 'hello',
    'host': 'localhost'
}


def get_schema_names(cursor):
    query = "SELECT schema_name FROM information_schema.schemata;"
    cursor.execute(query)
    return [row[0] for row in cursor.fetchall()]

def process_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)
    
def insert_data(data, table_name, cursor,conn):
    for entry in data:
        columns = ', '.join(entry.keys())
        placeholders = ', '.join(['%s'] * len(entry))
        values = tuple(entry.values())
        query = f"INSERT INTO GEOGRAPHY.{table_name} ({columns}) VALUES ({placeholders})"
        try:
            cursor.execute(query, values)
        except Exception as e:
            # print(f"Error executing query: {e}")
            # print("Failed query:", query)
            # print("With values:", values)
            print(e)
        conn.commit()
  
def base_tables_1():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Get schema names
        schema_names = get_schema_names(cursor)
        print("Schema Names:")
        for name in schema_names:
            print(name)        

        for file, [table, funcToApply] in file_table_mapping.items():
            print(f'processing {file_prefix+file}...')
            data = process_json_file(file_prefix+file)
            data1 = funcToApply(data)
            insert_data(data1, table, cursor,conn)            
            # break
        # Close the connection
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Database connection failed: {e}")
# ----------
        
def format_country_data(data):
    formatted_data = []
    for entry in data:
        formatted_entry = {
            'ID': entry.get('id'),
            'NAME': entry.get('name'),
            'ISO3': entry.get('iso3'),
            'NUMERIC_CODE': entry.get('numeric_code'),
            'ISO2': entry.get('iso2'),
            'PHONECODE': entry.get('phone_code'),
            'CAPITAL': entry.get('capital'),
            'CURRENCY': entry.get('currency'),
            'CURRENCY_NAME': entry.get('currency_name'),
            'CURRENCY_SYMBOL': entry.get('currency_symbol'),
            'TLD': entry.get('tld'),
            'NATIVE': entry.get('native'),
            'REGION': entry.get('region'),
            'REGION_ID': int(entry.get('region_id')) if entry.get('region_id') else None,
            'SUBREGION': entry.get('subregion'),
            'SUBREGION_ID': int(entry.get('subregion_id')) if entry.get('subregion_id') else None,
            'NATIONALITY': entry.get('nationality'),
            'TIMEZONES': json.dumps(entry.get('timezones', [])),
            'TRANSLATIONS': json.dumps(entry.get('translations', {})),
            'LATITUDE': float(entry.get('latitude')) if entry.get('latitude') else None,
            'LONGITUDE': float(entry.get('longitude')) if entry.get('longitude') else None,
            'EMOJI': entry.get('emoji'),
            'EMOJIU': entry.get('emojiU'),
            # CREATED_AT, UPDATED_AT, and FLAG are not included as they will be set by the database
        }
        formatted_data.append(formatted_entry)
    return formatted_data
def format_regions_data(regions_json):
    formatted_regions = []
    for region in regions_json:
        formatted_region = {
            'ID': region.get('id'),
            'NAME': region.get('name'),
            'TRANSLATIONS': json.dumps(region.get('translations', {})),
            'WIKIDATAID': region.get('wikiDataId'),
            # 'CREATED_AT', 'UPDATED_AT', and 'FLAG' are set by default or during the insertion
        }
        formatted_regions.append(formatted_region)
    return formatted_regions
def format_subregions_data(subregions_json):
    formatted_subregions = []
    for subregion in subregions_json:
        formatted_subregion = {
            'ID': subregion.get('id'),
            'NAME': subregion.get('name'),
            'REGION_ID': int(subregion.get('region_id')) if subregion.get('region_id') else None,
            'TRANSLATIONS': json.dumps(subregion.get('translations', {})),
            'WIKIDATAID': subregion.get('wikiDataId'),
            # 'CREATED_AT', 'UPDATED_AT', and 'FLAG' are set by default or during the insertion
        }
        formatted_subregions.append(formatted_subregion)
    return formatted_subregions
def format_cities_data(cities_json):
    formatted_cities = []
    for city in cities_json:
        formatted_city = {
            'ID': city.get('id'),
            'NAME': city.get('name'),
            'STATE_ID': city.get('state_id'),
            'STATE_CODE': city.get('state_code'),
            'COUNTRY_ID': city.get('country_id'),
            'COUNTRY_CODE': city.get('country_code'),
            'LATITUDE': float(city.get('latitude')) if city.get('latitude') else None,
            'LONGITUDE': float(city.get('longitude')) if city.get('longitude') else None,
            'WIKIDATAID': city.get('wikiDataId'),
            # 'CREATED_AT', 'UPDATED_AT', and 'FLAG' are set by default or during the insertion
        }
        formatted_cities.append(formatted_city)
    return formatted_cities
def format_states_data(states_json):
    formatted_states = []
    for state in states_json:
        formatted_state = {
            'ID': state.get('id'),
            'NAME': state.get('name'),
            'COUNTRY_ID': state.get('country_id'),
            'COUNTRY_CODE': state.get('country_code'),
            'FIPS_CODE': state.get('fips_code'),  # If this field is present in your JSON
            'ISO2': state.get('iso2'),  # If this field is present in your JSON
            'TYPE': state.get('type'),
            'LATITUDE': float(state.get('latitude')) if state.get('latitude') else None,
            'LONGITUDE': float(state.get('longitude')) if state.get('longitude') else None,
            'WIKIDATAID': state.get('wikiDataId'),  # If this field is present in your JSON
            # 'CREATED_AT', 'UPDATED_AT', and 'FLAG' are set by default or during the insertion
        }
        formatted_states.append(formatted_state)
    return formatted_states


# ----------
def to_upper_snake_case(name):
    # Replace specific terms and characters with underscores or words
    name = name.replace('/', '_PER_').replace('+', '_PLUS_').replace('.', '').replace("'", '').replace(',', '').replace('-', '_').replace('(', '').replace(')', '').replace(' ', '_')
    
    # Convert to snake case
    name = re.sub('(?<!^)(?=[A-Z])', '_', name).upper()
    name = re.sub(r'\s+', '_', name)
    name = re.sub(r'__+', '_', name)  # Replace multiple underscores with a single one
    
    # Specific replacements for clarity
    name = name.replace('TEMPERATURE_CO', 'TEMPERATURE_C')
    name = name.replace('PRECIPITATION_MILLIMETERS', 'PRECIPITATION_MM')
    name = name.replace('DECADEAL_AVERAGE', 'DECADE_AVERAGE')
    name = name.replace('DECADEAL_VARIATION', 'DECADE_VARIATION')
    name = name.replace('ANOMALY_CO', 'ANOMALY_C')
    name = name.replace('ANOMALY_MILLIMETERS', 'ANOMALY_MM')
    name = name.replace('PERCENT', 'PCT')
    name = name.replace('NUMBERS_PER', 'NUMBERS_PER_')
    
    # Ensure the name is uppercase
    name = name.upper()
    
    return name

def download_csv(url):
    response = requests.get(url)
    response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
    return response.text

def get_country_id_for_india(cursor):
    cursor.execute("SELECT ID FROM GEOGRAPHY.COUNTRIES WHERE NAME = 'India';")
    result = cursor.fetchone()
    return result[0] if result else None

def get_state_id(state_name, country_id, cursor):
    query = "SELECT ID FROM GEOGRAPHY.STATES WHERE REPLACE(LOWER(NAME), ' ', '') = REPLACE(LOWER(%s), ' ', '') AND COUNTRY_ID = %s;"
    cursor.execute(query, (state_name, country_id))
    result = cursor.fetchone()
    return result[0] if result else None

def get_districts_id(district_name,state_id, country_id, cursor):
    query = "SELECT ID FROM GEOGRAPHY.DISTRICTS WHERE REPLACE(LOWER(NAME), ' ', '') = REPLACE(LOWER(%s), ' ', '') AND STATE_ID = %s AND COUNTRY_ID = %s;"
    cursor.execute(query, (district_name, state_id, country_id))
    result = cursor.fetchone()
    return result[0] if result else None

def get_cities_id(name,state_id, country_id, cursor):
    query = "SELECT ID FROM GEOGRAPHY.CITIES WHERE REPLACE(LOWER(NAME), ' ', '') = REPLACE(LOWER(%s), ' ', '') AND STATE_ID = %s AND COUNTRY_ID = %s;"
    cursor.execute(query, (name, state_id, country_id))
    result = cursor.fetchone()
    return result[0] if result else None

def id_sequence_fix(cursor, table_name = 'GEOGRAPHY.STATES'):
    query = f"SELECT MAX(ID) FROM {table_name};"
    cursor.execute(query)
    result = cursor.fetchone()
    val =  result[0] if result else None
    query = f"ALTER SEQUENCE {table_name}_ID_SEQ  RESTART WITH {result+1};"
    cursor.execute(query)

def insert_state(state_name, country_id, cursor):
    # Check if the state already exists
    cursor.execute("SELECT ID FROM GEOGRAPHY.STATES WHERE REPLACE(LOWER(NAME), ' ', '') = REPLACE(LOWER(%s), ' ', '') AND COUNTRY_ID = %s;", (state_name, country_id))
    if cursor.fetchone():
        print(f"State '{state_name}' already exists in the database.")
        return
    # Insert the new state
    try:
        cursor.execute("INSERT INTO GEOGRAPHY.STATES (NAME, COUNTRY_ID, COUNTRY_CODE) VALUES (%s, %s,%s);", (state_name, country_id, 'IN'))
    except Exception as e:
        id_sequence_fix(cursor, 'GEOGRAPHY.STATES')
        print("INSERT INTO GEOGRAPHY.STATES (NAME, COUNTRY_ID, COUNTRY_CODE) VALUES (%s, %s,%s);".format(state_name, country_id, 'IN'))     
    print(f"State '{state_name}' inserted.")

def insert_district(district_name,state_name, country_id, cursor):
    # Check if the state already exists
    state_id = get_state_id(state_name=state_name,country_id=country_id,cursor=cursor)
    if not state_id:
        raise Exception(f"NAME= {state_name} AND COUNTRY_ID= {country_id} not found")        
    insert_query = """
    INSERT INTO GEOGRAPHY.DISTRICTS (NAME, STATE_ID, COUNTRY_ID, FLAG)
    VALUES (%s, %s, %s, %s);
    """
    cursor.execute(insert_query, (district_name, state_id, country_id, True))
    # print(f"District '{district_name}' inserted.")

def insert_cities_town(city_name,divison, district_name, state_name, country_id, cursor):
    # Check if the state already exists
    state_id = get_state_id(state_name=state_name,country_id=country_id,cursor=cursor)
    d_id = get_districts_id(district_name,state_id, country_id,cursor)
    try:
        C_id = get_cities_id(city_name,state_id, country_id,cursor)
    except:
        print(f"Error at {[city_name,state_id, country_id]}")
        C_id = None
    if not city_name or city_name == np.nan or city_name in ('nan',"NAN",'NaN', None, ''):
        return 
    if C_id:
        insert_query = """
        INSERT INTO GEOGRAPHY.INDIAN_CITIES_TOWNS (NAME, DIVISON, CITIES_ID, DISTRICT_ID,  STATE_ID, COUNTRY_ID, FLAG)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, (city_name,divison, C_id,d_id,state_id, country_id, True))
    else:
        insert_query = """
        INSERT INTO GEOGRAPHY.INDIAN_CITIES_TOWNS (NAME, DIVISON, CITIES_ID, DISTRICT_ID,  STATE_ID, COUNTRY_ID, FLAG)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, (city_name,divison, None, d_id,state_id, country_id, True))
    # print(f"CITY TOWN '{city_name}' inserted.")

def insert_zipcode(zip_code, place, state_name, province, province_code, community, community_code, latitude, longitude, country_id, cursor):
    # Check if the state already exists and get its ID
    state_id = get_state_id(state_name=state_name, country_id=country_id, cursor=cursor)
    if not state_id:
        raise Exception(f"State name '{state_name}' with country ID '{country_id}' not found")
    
    # Prepare the insert query
    insert_query = """
    INSERT INTO GEOGRAPHY.ZIPCODES 
    (ZIP_CODE, PLACE, STATE_ID, PROVINCE, PROVINCE_CODE, COMMUNITY, COMMUNITY_CODE, LATITUDE, LONGITUDE, FLAG)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(insert_query, (zip_code, place, state_id, province, province_code, community, community_code, latitude, longitude, True))
    # print(f"Zipcode '{zip_code}' inserted.")


def process_states_districts_towns(data):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    india_country_id = get_country_id_for_india(cursor)
    unique_states = data['LEVEL_1_NAME'].unique() 
    if india_country_id is None:
        print("India not found in the COUNTRIES table.")
    for name in tqdm(unique_states, 
                     desc="Inserting states"):
        insert_state(name, country_id=india_country_id, cursor=cursor)
        conn.commit()
    
    data2 = data.drop_duplicates(['LEVEL_2_NAME','LEVEL_1_NAME'])   
    for name in tqdm(data2[['LEVEL_1_NAME','LEVEL_2_NAME']].to_dict('records'), 
                     desc="Inserting districts"):
        insert_district(district_name = name['LEVEL_2_NAME'],
                        state_name= name['LEVEL_1_NAME'], 
                        country_id =india_country_id, cursor=cursor)   
        conn.commit() 
    
    data2 = data.drop_duplicates(['TOTAL_P_E_R_RURAL_P_E_R_URBAN_DIVISION', 'LEVEL_2_NAME','LEVEL_1_NAME','LEVEL_3_NAME'])   
    for name in tqdm(data2[['LEVEL_1_NAME','LEVEL_2_NAME','LEVEL_3_NAME','TOTAL_P_E_R_RURAL_P_E_R_URBAN_DIVISION']].to_dict('records'), 
                     desc="Inserting cities and towns"):
            try:
                insert_cities_town(city_name = name['LEVEL_3_NAME'],
                                divison = name['TOTAL_P_E_R_RURAL_P_E_R_URBAN_DIVISION'],
                                district_name = name['LEVEL_2_NAME'],
                                state_name= name['LEVEL_1_NAME'],                            
                                country_id =india_country_id, 
                                cursor=cursor)   
                conn.commit() 
            except Exception as e:
                print(e)
                print(name)
    cursor.close()
    conn.close()

def base_tables_2():
    #using india dataset only
    csv_url = 'https://datacatalogfiles.worldbank.org/ddh-published/0062657/DR0089000/2011-IND-L3.csv?versionId=2023-01-18T18:42:50.5855249Z'
    # Download the CSV file from the URL
    csv_data = download_csv(csv_url)
    # print(csv_data)
    data = pd.read_csv(csv_url, encoding='ISO-8859-1')
    new_names = {}
    for name in data.columns:
        new_names[name] = to_upper_snake_case(name)
    data.rename(columns=new_names,inplace=True)
    process_states_districts_towns(data)

# ---
def download_and_extract_zip(url):
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        for zipinfo in thezip.infolist():
            if zipinfo.filename.endswith('.csv'):
                thezip.extract(zipinfo)
                return zipinfo.filename 

def process_zipcodes(data):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    india_country_id = get_country_id_for_india(cursor)
    data['COUNTRY_ID'] = india_country_id

    for row in tqdm(data.to_dict('records'),
                        "Processing Zipcode"):
        try:
            insert_zipcode(zip_code=row['zipcode'], 
                        place=row['place'], 
                        state_name=row['state'], 
                        province=row['province'], 
                        province_code=row['province_code'], 
                        community=row['community'], 
                        community_code=row['community_code'], 
                        latitude=row['latitude'], 
                        longitude=row['longitude'], 
                        country_id=row['COUNTRY_ID'], 
                        cursor=cursor)
            conn.commit()
        except Exception as e:
            # print(e)
            # print(row)
            conn.rollback()

    cursor.close()
    conn.close()

def base_tables_3(): 
    url = "https://github.com/zauberware/postal-codes-json-xml-csv/blob/master/data/IN.zip?raw=true"
    csv_file_name = download_and_extract_zip(url)
    if csv_file_name:
        df = pd.read_csv(csv_file_name)

    process_zipcodes(df)

#  --- 

# Process and insert for each file
            
file_prefix = "countries-states-cities-database/"
file_table_mapping = {
    'regions.json': ['regions',format_regions_data],
    'subregions.json':  ['SUBREGIONS',format_subregions_data],
    'countries.json': ['COUNTRIES',format_country_data],
    'states.json': ['STATES',format_states_data],  
    'cities.json': ['CITIES',format_cities_data],    
}


base_tables_1()
base_tables_2()
base_tables_3()


# Clean files 
def delete_folder_if_exists(folder_path):
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        for root, dirs, files in os.walk(folder_path, topdown=False):
            for name in files:
                file_path = os.path.join(root, name)
                os.remove(file_path)
            for name in dirs:
                dir_path = os.path.join(root, name)
                if ".git" not in dir_path:  # Skip .git directory
                    shutil.rmtree(dir_path)
        shutil.rmtree(folder_path)  # Finally, remove the main directory
        print(f"Folder '{folder_path}' has been deleted.")
    else:
        print(f"Folder '{folder_path}' does not exist.")

def delete_file_if_exists(file_path):
    if os.path.exists(file_path) and os.path.isfile(file_path):
        os.remove(file_path)
        print(f"File '{file_path}' has been deleted.")
    else:
        print(f"File '{file_path}' does not exist.")

delete_folder_if_exists('countries-states-cities-database')
delete_file_if_exists('zipcodes.in.csv')