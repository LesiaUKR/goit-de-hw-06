from confluent_kafka import Producer
import json
import time
import random

producer = Producer({'bootstrap.servers': 'localhost:9092'})
topic = 'sensor_data'


def generate_message():
    """
    Генерує випадкове повідомлення із значеннями id, temperature, humidity, timestamp.
    """
    return {
        "id": random.randint(1, 100),
        "temperature": random.uniform(20, 80),
        "humidity": random.uniform(30, 70),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }


if __name__ == "__main__":
    while True:
        # Генеруємо повідомлення
        message = generate_message()

        # Виводимо повідомлення в консоль
        print("Generated message:", message)

        # Відправляємо повідомлення у Kafka
        producer.produce(topic, key=str(message["id"]), value=json.dumps(message))

        # Примусово очищуємо буфер
        producer.flush()

        # Пауза перед наступним повідомленням
        time.sleep(5)  # Інтервал між повідомленнями
