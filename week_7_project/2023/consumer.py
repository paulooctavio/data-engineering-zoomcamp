from kafka import KafkaConsumer
from json import loads
from time import sleep

# Getting the data as JSON
consumer = KafkaConsumer('data-stream',
bootstrap_servers=['localhost:9092'],
value_deserializer=lambda m: loads(m.decode('ascii')))

for message in consumer:
    price = (message.value)['data']['amount']
    print('Bitcoin price: ' + price)

# ssh into vm to run spark code
# test spark code