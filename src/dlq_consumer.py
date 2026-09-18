from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField


KAFKA_SERVER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
DLQ_TOPIC = "orders-dlq"


def load_avro_schema():
    project_directory = Path(__file__).resolve().parent.parent
    schema_path = project_directory / "schemas" / "order.avsc"

    with open(schema_path, "r", encoding="utf-8") as schema_file:
        return schema_file.read()


def get_header(message, header_name, default_value="Unknown"):
    headers = message.headers() or []

    for key, value in headers:
        if key == header_name:
            if isinstance(value, bytes):
                return value.decode("utf-8")

            return value

    return default_value


def main():
    registry_client = SchemaRegistryClient(
        {
            "url": SCHEMA_REGISTRY_URL
        }
    )

    deserializer = AvroDeserializer(
        registry_client,
        load_avro_schema()
    )

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_SERVER,
            "group.id": "dlq-monitor-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True
        }
    )

    consumer.subscribe([DLQ_TOPIC])

    print("DLQ consumer started.")
    print(f"Listening to: {DLQ_TOPIC}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                raise KafkaException(message.error())

            order = deserializer(
                message.value(),
                SerializationContext(
                    message.topic(),
                    MessageField.VALUE
                )
            )

            retry_count = get_header(
                message,
                "retry_count",
                "0"
            )

            failure_reason = get_header(
                message,
                "failure_reason"
            )

            original_topic = get_header(
                message,
                "original_topic"
            )

            print("=" * 55)
            print("DEAD LETTER QUEUE MESSAGE")
            print(f"Order ID       : {order['orderId']}")
            print(f"Product        : {order['product']}")
            print(f"Price          : Rs.{float(order['price']):.2f}")
            print(f"Retry count    : {retry_count}")
            print(f"Failure reason : {failure_reason}")
            print(f"Original topic : {original_topic}")
            print("=" * 55)

    except KeyboardInterrupt:
        print("\nDLQ consumer stopped by user.")

    finally:
        consumer.close()
        print("DLQ consumer closed.")


if __name__ == "__main__":
    main()