from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

def insert_data_into_postgres(t_path: str):
    """
    This function load the transformed data into PostgreSQL database.

    Parameters:
    t_path (str): path to the transformed file

    Returns: None
    """
    # load the transformed data from json file into country_data
    with open(t_path, "r") as f:
        country_data = json.load(f)
    # hooks - allows connection to postgres
    postgres_hook = PostgresHook(postgres_conn_id="postgres_worldcountry")
    #write insert query
    insert_query = """
    INSERT INTO WorldCountries (country_name, independence, UN_member, startOfWeek, 
                                official_name, nativeName, currency_code, currency_name, 
                                currency_symbol, country_code, capital, region, subregion, 
                                language, no_languages, area, population, continent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for country in country_data:
        postgres_hook.run(
            insert_query,
            parameters=(
                country["Country Name"],
                country["Independence"],
                country["UN Member"],
                country["startOfWeek"],
                country["Official name"],
                country["Common nativeName"],
                country["Currency code"],
                country["Currency name"],
                country["Currency symbol"],
                country["Country code"],
                country["Capital"],
                country["Region"],
                country["Subregion"],
                country["Official language"],
                country["No Languages"],
                country["Area"],
                country["Population"],
                country["Continent"],
            ),
        )