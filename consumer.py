import json
from confluent_kafka import Consumer

server = "127.0.0.1:9092"
TOPIC = "Transaction"

consumer = Consumer({
        "bootstrap.servers": server,
        "client.id": "cosumer_1",
        "broker.address.family": "v4",
        "group.id": "transactionGroup",
        "auto.offset.reset": "earliest"
}
)


def main():
    consumer.subscribe([TOPIC])
    try:
        while(True):
            massage = consumer.poll(1.0)
            if massage is  None:
                continue
            elif massage.error():
                print("У нас есть ошибка")
                continue    
            else:
                massage_2 = json.loads(
                massage.value().decode("utf-8")
                )
                print(massage_2)
            
    except KeyboardInterrupt:
        print("Остановка консьюмера")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()