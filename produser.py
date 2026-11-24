from confluent_kafka import Producer
import json
import random as rd
import uuid
import time

server = "localhost:9092"
Topic = "Transaction"
interval = 5

prod = Producer({
    "bootstrap.servers":server,
    "client.id": "transaction-producer",
    "acks":"all"
})

def create_massage():
    
    message = {
        "id":str(uuid.uuid4()),
        "sum":str(rd.randint(100,1000)),
        "count":str(rd.randint(1,10)),
        "total":str(rd.randint(20,30)),
        "name":rd.choice(["Anna", "Olga", "igor", "kot"]),
        "type":rd.choice(["Оплата","Перевод"])
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
    if message["type"] == "Оплата":
        Partiteon = 0
    else:
        Partiteon = 1

    prod.produce(topic = Topic, key=message["id"], value = json.dumps(message).encode("utf-8"), partition=Partiteon, callback=delivery)
    prod.poll(0)



def main():
    try:
        while True:
            message = create_massage()
            mart(message)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Конец отправки")
    finally:
        prod.flush()


if __name__ == "__main__":
    main()