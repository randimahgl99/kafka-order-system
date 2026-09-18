# Kafka Avro Order Processing System

## Overview

This project implements a Kafka-based order processing system using
Python, Apache Kafka, Avro and Confluent Schema Registry.

The system supports:

- Avro message serialization
- Real-time order processing
- Running average price calculation
- Retry logic for temporary failures
- Dead Letter Queue for permanent failures
- Manual Kafka offset management

## Order Schema

Each order contains:

| Field | Type | Description |
|---|---|---|
| orderId | string | Unique order identifier |
| product | string | Product name |
| price | float | Randomized product price |

Example:

```json
{
  "orderId": "1001",
  "product": "Laptop",
  "price": 1250.50
}
