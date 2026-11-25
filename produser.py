import json
import random
import uuid
import time

from confluent_kafka import Producer


BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC = "Transaction"
INTERVAL = 5


producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": "transaction-producer",
    "acks": "all",
    "broker.address.family": "v4"
})


def create_message():
    
    message = {
        "id": str(uuid.uuid4()),
        "sum": random.randint(100, 1000),
        "count": random.randint(1, 10),
        "total": random.randint(20, 30),
        "name": random.choice(["Anna", "Olga", "Igor", "Kot"]),
        "type": random.choice(["Оплата", "Перевод"])
    }

    return message


def delivery_report(err, msg):

    if err is not None:
        print(f"Ошибка доставки: {err}")

    else:
        print(
            f"Доставлено | "
            f"Topic: {msg.topic()} | "
            f"Partition: {msg.partition()} | "
            f"Offset: {msg.offset()}"
        )


def send_message(message):

    if message["type"] == "Оплата":
        partition = 0

    elif message["type"] == "Перевод":
        partition = 1

    else:
        raise ValueError("Неизвестный тип транзакции")

    producer.produce(
        topic=TOPIC,
        key=message["id"],
        value=json.dumps(
            message,
            ensure_ascii=False
        ).encode("utf-8"),
        partition=partition,
        callback=delivery_report
    )

    producer.poll(0)


def main():

    print("Producer запущен")

    try:
        while True:

            message = create_message()

            print(f"Отправляем: {message}")

            send_message(message)

            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("Конец отправки")

    finally:
        producer.flush()


if __name__ == "__main__":
    main()
