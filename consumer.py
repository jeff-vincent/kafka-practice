from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

MONGO_URI = 'mongodb+srv://jeff:AsspWord@cluster0-qnmim.mongodb.net/media?retryWrites=true&w=majority'

client = MongoClient(MONGO_URI)
collection = client.db.media

for message in consumer:
    message = message.value
    collection.insert_one(message)
    print('{} added to {}'.format(message, collection))

