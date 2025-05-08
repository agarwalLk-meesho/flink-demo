# Kafka-Flink-MongoDB Prototype

This prototype demonstrates a simple streaming pipeline that:
1. Reads transaction data from Kafka
2. Processes and aggregates the data using Apache Flink
3. Stores the aggregates in MongoDB

## Prerequisites

- Docker and Docker Compose
- Java 11 or higher
- Maven

## Setup and Running

1. Start the services:
```bash
chmod +x setup.sh
./setup.sh
```

2. Build the Flink job:
```bash
mvn clean package
```

3. Run the Flink job:
```bash
java -jar target/kafka-flink-mongo-1.0-SNAPSHOT.jar
```

4. FLink dashboard ti view Flink jobs
<img width="1728" alt="96153f83-a095-4236-a74e-5d7497673f0e" src="https://github.com/user-attachments/assets/6d709634-ab4d-4d8c-a25f-e1748f1ed2f8" />

## Testing the Pipeline

You can produce sample transaction data to Kafka using the following command:

```bash
docker-compose exec kafka kafka-console-producer --topic transactions --bootstrap-server localhost:9092
```

Sample transaction JSON format:
```json
{
  "transactionId": "tx123",
  "userId": "user1",
  "amount": 100.50,
  "category": "groceries",
  "timestamp": 1234567890
}
```

## Architecture

- Kafka Topic: `transactions`
- MongoDB Database: `flinkdb`
- MongoDB Collection: `transaction_aggregates`

The Flink job:
- Reads from Kafka topic `transactions`
- Groups transactions by category
- Creates 5-minute windows
- Calculates aggregates (total amount and count) per category
- Writes results to MongoDB

## Cleanup

To stop all services:
```bash
docker-compose down -v
``` 
