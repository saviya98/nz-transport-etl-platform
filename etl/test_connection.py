from sqlalchemy import create_engine
from config.db_config import DB_CONFIG

def test_connection():
    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    engine = create_engine(connection_string)

    try:
        with engine.connect() as conn:
            print("Database connection successful!")
    except Exception as e:
        print("Connection failed:", e)

if __name__ == "__main__":
    test_connection()
