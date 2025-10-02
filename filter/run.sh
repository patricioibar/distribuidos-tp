#!/bin/bash

# Run script for Filter Service with Docker

set -e

# Default values
FILTER_ID="filter-worker-1"
WORKERS_COUNT="3"
FILTER_TYPE="TbyYear"
CONSUMER_NAME="filter-consumer"
MW_ADDRESS="amqp://guest:guest@localhost:5672/"
SOURCE_QUEUE="transactions"
OUTPUT_EXCHANGE="filtered-transactions"
IMAGE_NAME="filter-service:latest"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --filter-id)
      FILTER_ID="$2"
      shift 2
      ;;
    --workers-count)
      WORKERS_COUNT="$2"
      shift 2
      ;;
    --filter-type)
      FILTER_TYPE="$2"
      shift 2
      ;;
    --consumer-name)
      CONSUMER_NAME="$2"
      shift 2
      ;;
    --mw-address)
      MW_ADDRESS="$2"
      shift 2
      ;;
    --source-queue)
      SOURCE_QUEUE="$2"
      shift 2
      ;;
    --output-exchange)
      OUTPUT_EXCHANGE="$2"
      shift 2
      ;;
    --image)
      IMAGE_NAME="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --filter-id ID          Filter worker ID (default: $FILTER_ID)"
      echo "  --workers-count COUNT   Number of workers (default: $WORKERS_COUNT)"
      echo "  --filter-type TYPE      Filter type: TbyYear|TbyHour|TbyAmount|TIbyYear (default: $FILTER_TYPE)"
      echo "  --consumer-name NAME    RabbitMQ consumer name (default: $CONSUMER_NAME)"
      echo "  --mw-address ADDR       RabbitMQ address (default: $MW_ADDRESS)"
      echo "  --source-queue QUEUE    Input queue name (default: $SOURCE_QUEUE)"
      echo "  --output-exchange EXCH  Output exchange name (default: $OUTPUT_EXCHANGE)"
      echo "  --image IMAGE           Docker image name (default: $IMAGE_NAME)"
      echo "  -h, --help              Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

echo "🚀 Starting Filter Service with Docker..."
echo "   Filter ID: $FILTER_ID"
echo "   Workers Count: $WORKERS_COUNT"
echo "   Filter Type: $FILTER_TYPE"
echo "   Consumer: $CONSUMER_NAME"
echo "   MW Address: $MW_ADDRESS"
echo "   Source Queue: $SOURCE_QUEUE"
echo "   Output Exchange: $OUTPUT_EXCHANGE"
echo "   Image: $IMAGE_NAME"
echo ""

# Run the container
docker run --rm -it \
  -e FILTER_ID="$FILTER_ID" \
  -e WORKERS_COUNT="$WORKERS_COUNT" \
  -e FILTER_TYPE="$FILTER_TYPE" \
  -e CONSUMER_NAME="$CONSUMER_NAME" \
  -e MW_ADDRESS="$MW_ADDRESS" \
  -e SOURCE_QUEUE="$SOURCE_QUEUE" \
  -e OUTPUT_EXCHANGE="$OUTPUT_EXCHANGE" \
  --name "filter-$FILTER_ID" \
  "$IMAGE_NAME"