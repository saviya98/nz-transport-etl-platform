import requests
import pandas as pd
from sqlalchemy import create_engine
from config.db_config import DB_CONFIG

def extract_weather_data():
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": -36.8485, #Auckland
        "longitude": 174.7633,
        "current_weather": True,
        }
    
    response = requests.get(url, params=params)
    data = response.json()
    print("Extracted weather data:", data)
    return data

def transform_weather_data(data):
    current_weather = data["current_weather"]
    
    df = pd.DataFrame([
        {"temperature": current_weather["temperature"],
         "windspeed": current_weather["windspeed"],
         "timestamp": current_weather["time"]}
    ])

    df["rainfall"] = 0.0
    return df

def load_weather_data(df):
    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    engine = create_engine(connection_string)

    try:
        df.to_sql('fact_weather', engine, if_exists='append', index=False)
        print("Weather data loaded successfully!")
    except Exception as e:
        print("Failed to load data:", e)    


def main():
    weather_data = extract_weather_data()
    df = transform_weather_data(weather_data)
    load_weather_data(df)   

    print("Weather ETL process completed successfully!")

if __name__ == "__main__":
    main()