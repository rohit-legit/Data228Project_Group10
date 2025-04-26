# news_producer.py 

import pandas as pd
import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv("financialphrasebank.csv") 

title_templates = [
    "Market Insight: {}",
    "Stock Update: {}",
    "Finance News: {}",
    "Investor Alert: {}",
    "Trending Now: {}",
    "Breaking: {}",
    "Hot Stocks: {}",
    "Financial Brief: {}",
]

print("Loaded offline dataset with", len(df), "records")

while True:
    sampled = df.sample(50)  
    for _, row in sampled.iterrows():
        phrase = str(row.iloc[0])  
        random_title = random.choice(title_templates).format(phrase[:40])
        random_days_back = random.randint(0, 90)
        fake_date = (pd.Timestamp.today() - pd.Timedelta(days=random_days_back)).strftime('%Y-%m-%dT%H:%M:%SZ')

        msg = {
            "title": random_title,
            "content": phrase,
            "publishedAt": fake_date
        }
        producer.send("offlinenewsdata", msg)
        print(f"Sent offline news:", msg["title"], "| Published At:", msg["publishedAt"])
    
    time.sleep(10)  
