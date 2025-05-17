import pandas as pd
import json


def transform(e_path: str, t_path: str):
    """
    This function transform the json file extracted from Rest Countries API.
    
    Parameters:
    e_path (str): path to the extracted data
    t_path(str): path to the file where the transformed data will be saved

    Returns: a json file
    """

    # open the extracted json file and load the content
    with open(e_path, "r") as f:
        data = json.load(f)

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
            "Continent": continent,
        }
        country_info.append(country_dict)

        # load the transformed data into json file
        with open(t_path, "w") as outfile:
            json.dump(country_info, outfile)
