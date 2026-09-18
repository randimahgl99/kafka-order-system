import json
import random
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField


KAFKA_SERVER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
ORDERS_TOPIC = "orders"


def delivery_report(error, message):
    """
    Kafka calls this function after attempting to deliver a message.
    """

    if error is not None:
        print(f"Delivery failed: {error}")
    else:
        print(
            f"Delivered to topic={message.topic()}, "
            f"partition={message.partition()}, "
            f"offset={message.offset()}"
        )


def load_avro_schema():
    """
    Read the order.avsc file.
    """

    project_directory = Path(__file__).resolve().parent.parent
    schema_path = project_directory / "schemas" / "order.avsc"

    with open(schema_path, "r", encoding="utf-8") as schema_file:
        return schema_file.read()


def create_producer():
    """
    Create the Kafka producer and Avro serializer.
    """

    schema_registry_client = SchemaRegistryClient(
        {
            "url": SCHEMA_REGISTRY_URL
        }
    )

    order_schema = load_avro_schema()

    avro_serializer = AvroSerializer(
        schema_registry_client,
        order_schema
    )

    kafka_producer = Producer(
        {
            "bootstrap.servers": KAFKA_SERVER,
            "client.id": "order-producer"
        }
    )

    return kafka_producer, avro_serializer


def generate_order(order_number):
    """
    Generate normal and failure-testing orders.
    """

    products = [
        "Laptop",
        "Keyboard",
        "Mouse",
        "Monitor",
        "Headphones"
    ]

    if order_number == 1003:
        # Fails twice and then succeeds.
        product = "RetryItem"

    elif order_number == 1004:
        # Permanent failure: goes directly to DLQ.
        product = "InvalidItem"

    elif order_number == 1005:
        # Temporary failure that never recovers.
        product = "AlwaysFail"

    else:
        product = random.choice(products)

    return {
        "orderId": str(order_number),
        "product": product,
        "price": round(random.uniform(100.00, 5000.00), 2)
    }


def main():
    producer, avro_serializer = create_producer()

    print("Starting order producer...")
    print("Press Ctrl+C to stop.\n")

    try:
        # Generate ten order messages.
        for order_number in range(1001, 1011):
            order = generate_order(order_number)

            serialized_order = avro_serializer(
                order,
                SerializationContext(
                    ORDERS_TOPIC,
                    MessageField.VALUE
                )
            )

            producer.produce(
                topic=ORDERS_TOPIC,
                key=order["orderId"].encode("utf-8"),
                value=serialized_order,
                callback=delivery_report
            )

            # Allows Kafka to execute delivery callbacks.
            producer.poll(0)

            print(
                f"Produced order: "
                f"ID={order['orderId']}, "
                f"Product={order['product']}, "
                f"Price=Rs.{order['price']:.2f}"
            )

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")

    finally:
        print("Waiting for remaining messages...")
        producer.flush()
        print("Producer finished.")


if __name__ == "__main__":
    main()