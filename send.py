from kafka import KafkaProducer, KafkaConsumer
import json
import time
from datetime import datetime

topic = 'test'

k_host = '192.168.NNN.NNN:9092'

if __name__ == "__main__":

    producer = KafkaProducer(bootstrap_servers=[k_host],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8')
                             )

    value = {'name':'test json', 'data': str(datetime.now())}
    producer.send(topic, value=value)
    
    producer.close()
