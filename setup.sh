#!/bin/bash
# Maven clean install
echo "Maven clean install..."
mvn clean install

# Start the services
echo "Stopping services..."
docker compose down --volumes --remove-orphans

# Start the services
echo "Starting services..."
docker-compose up -d

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 15

# Create Kafka topic
echo "Creating Kafka topic..."
docker-compose exec kafka kafka-topics --create --topic transactions --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

# Create MongoDB database and collection
echo "Setting up MongoDB..."
docker-compose exec mongodb mongosh --username admin --password password --eval 'db = db.getSiblingDB("flinkdb"); db.createCollection("transaction_aggregates");'

echo "Setup completed!" 

# Check if Kafka is ready
echo "Checking if Kafka is ready..."
# docker-compose exec jobmanager bash -c "apt-get update && apt-get install -y netcat-openbsd && nc -zv kafka 29092"

# Run the Flink job
echo "Running Flink job..."
# docker-compose exec jobmanager bash -c "flink run /opt/flink/target/kafka-flink-mongo-1.0-SNAPSHOT.jar"
# docker-compose exec jobmanager bash -c "flink run -Dlog.file=/dev/stdout /opt/flink/target/kafka-flink-mongo-1.0-SNAPSHOT.jar"
# java -jar target/kafka-flink-mongo-1.0-SNAPSHOT.jar | grep "Process Element"

# to restart the producer fresh
#    docker-compose exec kafka kafka-topics --delete --topic transactions --bootstrap-server kafka:9092
#    docker-compose exec kafka kafka-topics --create --topic transactions --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1

# Inside Flink Commands
# docker-compose exec jobmanager bash
# flink run /opt/flink/target/kafka-flink-mongo-1.0-SNAPSHOT.jar
# flink stop 9dfc524e964cab9355692b7dadc09105 -p /opt/flink/checkpoints
# flink run -s /opt/flink/checkpoints/savepoint-a063b9-80b227c30e4e -c com.example.Main /opt/flink/target/kafka-flink-mongo-1.0-SNAPSHOT.jar
