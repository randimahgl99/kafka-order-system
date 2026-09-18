import time
from pathlib import Path

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import (
    AvroDeserializer,
    AvroSerializer
)
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField
)


KAFKA_SERVER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

ORDERS_TOPIC = "orders"
RETRY_TOPIC = "orders-retry"
DLQ_TOPIC = "orders-dlq"

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2


class TemporaryOrderError(Exception):
    """A temporary failure that might succeed later."""


class PermanentOrderError(Exception):
    """A permanent failure that should not be retried."""


def load_avro_schema():
    project_directory = Path(__file__).resolve().parent.parent
    schema_path = project_directory / "schemas" / "order.avsc"

    with open(schema_path, "r", encoding="utf-8") as schema_file:
        return schema_file.read()


def create_kafka_clients():
    registry_client = SchemaRegistryClient(
        {
            "url": SCHEMA_REGISTRY_URL
        }
    )

    order_schema = load_avro_schema()

    serializer = AvroSerializer(
        registry_client,
        order_schema
    )

    deserializer = AvroDeserializer(
        registry_client,
        order_schema
    )

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_SERVER,
            "group.id": "order-processing-group-v2",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        }
    )

    failure_producer = Producer(
        {
            "bootstrap.servers": KAFKA_SERVER,
            "client.id": "failure-handler-producer"
        }
    )

    return consumer, failure_producer, serializer, deserializer


def get_header(message, header_name, default_value=None):
    headers = message.headers() or []

    for key, value in headers:
        if key == header_name:
            if isinstance(value, bytes):
                return value.decode("utf-8")

            return value

    return default_value


def process_order(order, retry_count):
    """
    Simulate successful, temporary and permanent processing.
    """

    product = order["product"]

    if product == "InvalidItem":
        raise PermanentOrderError(
            "The product is permanently invalid"
        )

    if product == "AlwaysFail":
        raise TemporaryOrderError(
            "External order service is still unavailable"
        )

    if product == "RetryItem" and retry_count < 2:
        raise TemporaryOrderError(
            "Temporary payment service failure"
        )


def publish_order(
    producer,
    serializer,
    topic,
    order,
    retry_count,
    failure_reason
):
    """
    Publish an Avro order to the retry or DLQ topic.
    """

    serialized_order = serializer(
        order,
        SerializationContext(
            topic,
            MessageField.VALUE
        )
    )

    delivery_errors = []

    def delivery_callback(error, message):
        if error is not None:
            delivery_errors.append(error)
        else:
            print(
                f"Published order {order['orderId']} to "
                f"{message.topic()} at offset "
                f"{message.offset()}."
            )

    headers = [
        (
            "retry_count",
            str(retry_count).encode("utf-8")
        ),
        (
            "failure_reason",
            failure_reason.encode("utf-8")
        ),
        (
            "original_topic",
            ORDERS_TOPIC.encode("utf-8")
        )
    ]

    producer.produce(
        topic=topic,
        key=order["orderId"].encode("utf-8"),
        value=serialized_order,
        headers=headers,
        callback=delivery_callback
    )

    undelivered_count = producer.flush(10)

    if undelivered_count > 0:
        raise RuntimeError(
            f"Could not publish order to {topic}"
        )

    if delivery_errors:
        raise RuntimeError(
            f"Kafka delivery failed: {delivery_errors[0]}"
        )


def print_successful_order(
    order,
    retry_count,
    total_price,
    order_count
):
    running_average = total_price / order_count

    print("-" * 50)
    print("ORDER PROCESSED SUCCESSFULLY")
    print(f"Order ID       : {order['orderId']}")
    print(f"Product        : {order['product']}")
    print(f"Price          : Rs.{float(order['price']):.2f}")
    print(f"Retry count    : {retry_count}")
    print(f"Processed count: {order_count}")
    print(f"Total price    : Rs.{total_price:.2f}")
    print(f"Running average: Rs.{running_average:.2f}")
    print("-" * 50)


def main():
    (
        consumer,
        failure_producer,
        serializer,
        deserializer
    ) = create_kafka_clients()

    consumer.subscribe(
        [
            ORDERS_TOPIC,
            RETRY_TOPIC
        ]
    )

    total_price = 0.0
    order_count = 0

    print("Order processing system started.")
    print(f"Maximum retries: {MAX_RETRIES}")
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

            order = None

            try:
                order = deserializer(
                    message.value(),
                    SerializationContext(
                        message.topic(),
                        MessageField.VALUE
                    )
                )

                retry_count = int(
                    get_header(
                        message,
                        "retry_count",
                        "0"
                    )
                )

                print(
                    f"\nReceived order {order['orderId']} "
                    f"from {message.topic()}; "
                    f"retry count={retry_count}"
                )

                process_order(order, retry_count)

                price = float(order["price"])
                total_price += price
                order_count += 1

                print_successful_order(
                    order,
                    retry_count,
                    total_price,
                    order_count
                )

                consumer.commit(
                    message=message,
                    asynchronous=False
                )

            except TemporaryOrderError as error:
                retry_count = int(
                    get_header(
                        message,
                        "retry_count",
                        "0"
                    )
                )

                if retry_count < MAX_RETRIES:
                    next_retry_count = retry_count + 1

                    print(f"Temporary failure: {error}")
                    print(
                        f"Scheduling retry "
                        f"{next_retry_count}/{MAX_RETRIES}"
                    )
                    print(
                        f"Waiting {RETRY_DELAY_SECONDS} seconds..."
                    )

                    time.sleep(RETRY_DELAY_SECONDS)

                    publish_order(
                        failure_producer,
                        serializer,
                        RETRY_TOPIC,
                        order,
                        next_retry_count,
                        str(error)
                    )

                else:
                    print(
                        f"Order {order['orderId']} exhausted "
                        f"all {MAX_RETRIES} retries."
                    )
                    print("Sending order to DLQ.")

                    publish_order(
                        failure_producer,
                        serializer,
                        DLQ_TOPIC,
                        order,
                        retry_count,
                        f"Maximum retries exceeded: {error}"
                    )

                # Commit only after retry/DLQ publishing succeeds.
                consumer.commit(
                    message=message,
                    asynchronous=False
                )

            except PermanentOrderError as error:
                retry_count = int(
                    get_header(
                        message,
                        "retry_count",
                        "0"
                    )
                )

                print(f"Permanent failure: {error}")
                print(
                    f"Sending order {order['orderId']} "
                    f"directly to DLQ."
                )

                publish_order(
                    failure_producer,
                    serializer,
                    DLQ_TOPIC,
                    order,
                    retry_count,
                    str(error)
                )

                consumer.commit(
                    message=message,
                    asynchronous=False
                )

            except Exception as error:
                print(f"Unexpected error: {error}")

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    finally:
        failure_producer.flush()
        consumer.close()
        print("Kafka clients closed.")


if __name__ == "__main__":
    main()