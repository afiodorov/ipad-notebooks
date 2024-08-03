import os
import json
import time
from pathlib import Path
from urllib.parse import urlencode
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from decimal import Decimal

import requests
from requests.adapters import HTTPAdapter, Retry
import pandas as pd

from mylib.cache import cache

load_dotenv()

def get_token():
    url = "https://test.api.amadeus.com/v1/security/oauth2/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": os.environ['AMADEUS_API_TOKEN'],
        "client_secret": os.environ['AMADEUS_API_SECRET'],
    }
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.json()['access_token']


def create_session_with_retries(
    retries=3,
    backoff_factor=1,
):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_airline_name(carrier_code, dictionaries):
    """
    Get the airline name from the carrier code using the dictionaries in the API response.
    """
    carriers = dictionaries.get('carriers', {})
    return carriers.get(carrier_code, carrier_code)

def parse_flight_offers(json_data):
    offers = json_data.get('data', [])
    dictionaries = json_data.get('dictionaries', {})
    parsed_offers = []

    for offer in offers:
        price = offer['price']['total']
        currency = offer['price']['currency']
        
        for itinerary in offer['itineraries']:
            for segment in itinerary['segments']:
                carrier_code = segment['carrierCode']
                airline_name = get_airline_name(carrier_code, dictionaries)
                departure_time = segment['departure']['at']
                arrival_time = segment['arrival']['at']
                departure_iata = segment['departure']['iataCode']
                arrival_iata = segment['arrival']['iataCode']
                departure_terminal = segment['departure'].get('terminal', 0)
                arrival_terminal = segment['arrival'].get('terminal', 0)
                
                parsed_offers.append({
                    'airline': airline_name,
                    'departure': f"{departure_time}",
                     't_from': departure_terminal,
                    'arrival': f"{arrival_time}",
                    f'price_{currency.lower()}': Decimal(price),
                    't_to': arrival_terminal,
                })

    return parsed_offers

token = get_token()
session = create_session_with_retries()

@cache()
def get(*args, sleep=True):
    h = {'Authorization': f"Bearer {token}"}
    r = session.get(*args, headers=h)
    if sleep:
        time.sleep(0.1)
    return r

def get_flights(*, start: str, end: str, when: str, range: int = 1):
    res = []
    
    cur_date = date.fromisoformat(when)
    end_date = cur_date + timedelta(days=range)
    
    while cur_date < end_date:
        p = urlencode(
            {
                'originLocationCode': start,
                'destinationLocationCode': end,
                'departureDate': cur_date.isoformat(),
                "nonStop": "true",
                "adults": 1,
                'currencyCode': "EUR",
            }
        )
        should_sleep: bool = cur_date + timedelta(days=1) != end_date
        response = get(
            f"https://test.api.amadeus.com/v2/shopping/flight-offers?{p}",
            sleep=should_sleep,
        )
        response.raise_for_status()
        
        res.extend(parse_flight_offers(response.json()))
        cur_date += timedelta(days=1)
    
    res = sorted(res, key=lambda x: (x['departure'], x['airline']))    
    df = pd.DataFrame.from_records(res)
    return df