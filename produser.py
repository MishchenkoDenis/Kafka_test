from confluent_kafka import Producer
import json
import random as rd
import uuid

server = "localhost:9092"
topic = "Transaction"
interval = 5

prod = Producer({
    "bootstrap.servers":server,
    "client.id": "transaction-producer",
    "acks":"all"
})

def create_massage():
    
    message = {
        "type":rd.choice([0,1]),
        "id":str(uuid.uuid4()),
          "sum":str(rd.randint(100,1000)),
          "count":str(rd.randint(1,10)),
          "total":str(rd.randint(20,30)),
          "name":rd.choice(["Anna", "Olga", "igor", "kot"])
          }
    return message


def delivery(err, txt):
    if err is not None:
        print(f"{err}")
    else:
        print(f"Статус сообщения: {txt.topic()}")
        print(f"Статус сообщения: {txt.partition()}")
        print(f"Статус сообщения: {txt.offset()}")

def mart(message):
    if message["type"] == 0:
        partiteon = 0
    else:
        partiteon = 1


    

