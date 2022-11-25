import time
import random
from kafka import KafkaProducer

bootstrap_server=["localhost:9092"]
topic="Naturalnumbers"
producer=KafkaProducer(bootstrap_servers=bootstrap_server)
producer=KafkaProducer()


def  senddata():
    random_number=random.randint(0,1000)
    
    message=producer.send(topic,bytes(str(random_number),"utf8"))
    metadata=message.get()
    print(metadata.topic)
    print(metadata.partition)
    
    time.sleep(5)
while True:
    senddata()