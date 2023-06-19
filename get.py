from kafka import KafkaProducer, KafkaConsumer
import json
import time
from datetime import datetime

topic = 'test'

k_host = '192.168.NNN.NNN:9092'

if __name__ == "__main__":

    consumer = KafkaConsumer(topic, 
                             group_id='my-group-N', 
                             bootstrap_servers=[k_host], 
                             #auto_offset_reset='earliest', # политика для сброса смещений при ошибках (default: latest)
                             #enable_auto_commit=False # автоматическое подтверждение о прочтении (default: True)
                             )

    for message in consumer:
        print(f'date = {str(datetime.now())}, message = {message}')
        try:
            data = json.loads(message.value)
        except Exception as e:
            print(str(e))
