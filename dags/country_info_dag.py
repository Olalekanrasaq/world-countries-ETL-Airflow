# Import libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json


# ETL pipeline

def extract(ti):
    url = "https://restcountries.com/v3.1/all" # rest api
    headers = {'Accepts': 'application/json'}
    session = Session()
    session.headers.update(headers)
    try:
        response = session.get(url)
        data = json.loads(response.text)
        ti.xcom_push(key="country_data", value=data) # push the data to xcom for storage
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)

def transform(ti):
    # pull data from xcom
    data = ti.xcom_pull(key="country_data", task_ids='extract_country_data') 
    country_info = []
    # loop through the country data and extract the fields of interest
    for country in data:
        try:
            c_name = country.get("name").get("common")
        except:
            c_name = None
        try:
            ind = bool(country.get("independent"))
        except:
            ind = None
        try:
            uN = bool(country.get("unMember"))
        except:
            uN = None
        try:
            s_week = country.get("startOfWeek")
        except:
            s_week = None
        try:
            o_name = country.get("name").get("official")
        except:
            o_name = None
        try:
            n_name = country.get("name").get("nativeName").get(list(country.get("name").get("nativeName").keys())[0]).get("common")
        except:
            n_name = None
        try:
            cur_code = list(country.get("currencies").keys())[0]
        except:
            cur_code = None
        try:
            cur_name = country.get("currencies").get(list(country.get("currencies").keys())[0]).get("name")
        except:
            cur_name = None
        try:
            cur_symb = country.get("currencies").get(list(country.get("currencies").keys())[0]).get("symbol")
        except:
            cur_symb = None
        try:
            c_code = f'{country.get("idd").get("root")}{country.get("idd").get("suffixes")[0]}'
        except:
            c_code = None
        try:
            capital = country.get("capital")[0]
        except:
            capital = None
        try:
            region = country.get("region")
        except:
            region = None
        try:
            s_region = country.get("subregion")
        except:
            s_region = None
        try:
            o_lang = country.get("languages").get(list(country.get("languages").keys())[0])
        except:
            o_lang = None
        try:
            n_lang = len(list(country.get("languages").keys()))
        except:
            n_lang = 0
        try:
            area = float(country.get("area"))
        except:
            area = None
        try:
            pop = int(country.get("population"))
        except:
            pop = None
        try:
            continent = country.get("continents")[0]
        except:
            continent = None

        country_dict = {
                "Country Name": c_name,
                "Independence": ind,
                "UN Member": uN,
                "startOfWeek": s_week,
                "Official name": o_name,
                "Common nativeName": n_name,
                "Currency code": cur_code,
                "Currency name": cur_name,
                "Currency symbol": cur_symb,
                "Country code": c_code,
                "Capital": capital,
                "Region": region,
                "Subregion": s_region,
                "Official language": o_lang,
                "No Languages": n_lang,
                "Area": area,
                "Population": pop,
                "Continent": continent
        }
        country_info.append(country_dict)
    
    df = pd.DataFrame(country_info) # convert the country info list to dataframe
    ti.xcom_push(key="dataframe", value=df.to_dict('records')) # push the dataframe to xcom

def insert_data_into_postgres(ti):
    country_data = ti.xcom_pull(key='dataframe', task_ids='parse_country_info')
    if not country_data:
        raise ValueError("No data found")
    #hooks - allows connection to postgres
    postgres_hook = PostgresHook(postgres_conn_id='postgres_worldcountry')
    insert_query = """
    INSERT INTO WorldCountries (country_name, independence, UN_member, startOfWeek, official_name, nativeName, currency_code, currency_name, currency_symbol, country_code, capital, region, subregion, language, no_languages, area, population, continent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for country in country_data:
        postgres_hook.run(insert_query, 
                          parameters=(country["Country Name"], country["Independence"], country["UN Member"], country["startOfWeek"], country["Official name"], country["Common nativeName"], country["Currency code"], country["Currency name"], country["Currency symbol"], country["Country code"], country["Capital"], country["Region"], country["Subregion"], country["Official language"], country["No Languages"], country["Area"], country["Population"], country["Continent"]))

default_args = {
    'owner': 'Akinkunmi',
    'start_date': days_ago(0),
    'email': ['olalekanrasaq1331@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'world_country_info',
    default_args=default_args,
    description='A DAG to fetch world country information and store it in Postgres database',
)


#operators : Python Operator and PostgresOperator

extract_data = PythonOperator(
    task_id='extract_country_data',
    python_callable=extract,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='parse_country_info',
    python_callable=transform,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_worldcountry',
    sql="""
    CREATE TABLE IF NOT EXISTS WorldCountries (
        id SERIAL PRIMARY KEY,
        country_name TEXT,
        independence BOOLEAN,
        UN_member BOOLEAN,
        startOfWeek TEXT,
        official_name TEXT,
        nativeName TEXT,
        currency_code TEXT,
        currency_name TEXT,
        currency_symbol TEXT,
        country_code TEXT,
        capital TEXT,
        region TEXT,
        subregion TEXT,
        language TEXT,
        no_languages INT,
        area NUMERIC(18,2),
        population BIGINT,
        continent TEXT
    );
    """,
    dag=dag,
)

insert_country_data = PythonOperator(
    task_id='insert_country_info',
    python_callable=insert_data_into_postgres,
    dag=dag,
)

# Task pipeline

extract_data >> transform_data >> create_table_task >> insert_country_data