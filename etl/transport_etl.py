import pandas as pd
from datetime import datetime
import random
from sqlalchemy import create_engine
from config.db_config import DB_CONFIG

def extract_transport_data():
    # Simulate transport data extraction
    data =[]

    routes = ["NX1", "NX2", "RBC", "OUT", "INN"]
    stops = ["Stop A", "Stop B", "Stop C"]

    for _ in range(10):
        data.append({
            "timestamp": datetime.now(),
            "route_id": random.choice(routes),
            "stop_id": random.choice(stops),
            "delay_minutes": round(random.uniform(-2, 15), 2)
        })
  
    print("Extracted transport data:", data)
    return pd.DataFrame(data)

def transform_transport_data(df):
    #Remove the early arrivals
    df = df[df["delay_minutes"] >= 0]

    #remove duplicates
    df = df.drop_duplicates()

    #standardize timestamps
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

def load_transport_data(df):
    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    engine = create_engine(connection_string)

    try:
        df.to_sql('fact_transport_delays', engine, if_exists='append', index=False)
        print("Transport data loaded successfully!")
    except Exception as e:
        print("Failed to load data:", e)

def main():
    transport_data = extract_transport_data()
    transformed_data = transform_transport_data(transport_data)
    load_transport_data(transformed_data)

    print("Transport ETL process completed successfully!")

if __name__ == "__main__":
    main()