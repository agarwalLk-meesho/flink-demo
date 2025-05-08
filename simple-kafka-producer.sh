#!/bin/bash

# Configuration
TOPIC="transactions"

echo "Starting simple Kafka transaction producer..."
echo "Press Ctrl+C to stop"

# Counter for timestamp
TIMESTAMP=1
COUNT=0

# Loop to generate and send messages
while true; do
  # Create fixed transaction with incrementing timestamp
  MSG="{\"transactionId\":\"tx123\",\"userId\":\"user1\",\"amount\":1,\"category\":\"groceries\",\"timestamp\":$TIMESTAMP}"
  
  # Print the message
  echo "Sending message #$COUNT with timestamp: $TIMESTAMP"
  echo "$MSG"

  # Send to Kafka
  docker-compose exec -T kafka kafka-console-producer --topic transactions --bootstrap-server localhost:9092 <<< "$MSG"

  # See the currently commited offset for the topic by the consumergroup
  docker-compose exec -T kafka kafka-consumer-groups --describe --bootstrap-server localhost:9092 --group flink-group
  
  # Increment counter and timestamp
  COUNT=$((COUNT + 1))
  TIMESTAMP=$((TIMESTAMP + 1000))
  
  # Sleep for 1 second
  sleep 1
done 