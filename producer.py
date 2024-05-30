import time
import json
from kafka import KafkaProducer
import pandas as pd
from json import JSONEncoder
import datetime

class CustomEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return JSONEncoder.default(self, obj)
        
def generate_transaction(row):
    transaction_data = {
        'date': row['Date'],
        'open': row['Open'],
        'high': row['High'],
        'low': row['Low'],
        'close': row['Close'],
        'volume': row['Volume']
    }
    return transaction_data
        
def produce_transactions():
    producer = KafkaProducer(bootstrap_servers='broker:9092')
    df = pd.read_csv('sp500.csv')
    df['Date'] = pd.to_datetime(df['Date'])
    df = df[df['Date'] > pd.Timestamp('1990-01-01')]
    while True:
        for index, row in df.iterrows():
            transaction = generate_transaction(row)
            transaction_bytes = json.dumps(transaction, cls=CustomEncoder).encode('utf-8')
            producer.send('transactions-topic', value=transaction_bytes)
            time.sleep(0.01)
            print(transaction_bytes)
        producer.flush()
            

if __name__ == '__main__':
    produce_transactions()