from config.read_config import load_config
from pymongo import MongoClient

def connect():
    try:
        mongodb_config = load_config()

        client = MongoClient(
            host=mongodb_config['host'],
            port=int(mongodb_config['port'])
        )
        db = client[mongodb_config['database']]
        collection = db[mongodb_config['collection']]
        
        return db, collection
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None, 

if __name__ == '__main__':
    connect()