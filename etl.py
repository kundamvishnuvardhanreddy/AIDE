import requests
import pandas as pd
from mongita import MongitaClientDisk

API_URL = "https://jsonplaceholder.typicode.com/posts"

def extract_api_data(url):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()

def clean_data(raw_data):
    df = pd.DataFrame(raw_data)
    df = df.dropna()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    return df.to_dict(orient="records")

def load_to_mongodb(records, db_name, collection_name):
    client = MongitaClientDisk()
    collection = client[db_name][collection_name]
    collection.insert_many(records)

def main():
    print("Starting ETL...")
    raw = extract_api_data(API_URL)
    cleaned = clean_data(raw)
    load_to_mongodb(cleaned, "etl_db", "posts")
    print("ETL completed successfully ✅")

if __name__ == "__main__":
    main()
