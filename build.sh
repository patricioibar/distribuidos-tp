#!/bin/bash

build_one() {
    local service=$1
    case $service in
        aggregator|joiner|filter|analyst|monitor)
            echo "Building $service..."
            sudo docker build -f $service/Dockerfile -t $service:latest .
            ;;
        coffee-analyzer)
            echo "Building coffee-analyzer..."
            sudo docker build -f coffee-analyzer/Dockerfile -t coffee-analyzer:latest .
            ;;
        *)
            echo "Error: Unknown service '$service'"
            echo "Available services: aggregator, joiner, filter, analyst, coffee-analyzer, monitor"
            exit 1
            ;;
    esac
}

build_all() {
    echo "Building all services..."
    sudo docker build -f aggregator/Dockerfile -t aggregator:latest .
    sudo docker build -f joiner/Dockerfile -t joiner:latest .
    sudo docker build -f filter/Dockerfile -t filter:latest .
    sudo docker build -f analyst/Dockerfile -t analyst:latest .
    sudo docker build -f coffee-analyzer/Dockerfile -t coffee-analyzer:latest .
    sudo docker build -f monitor/Dockerfile -t monitor:latest .
}

if [ -z "$1" ]; then
    build_all
else
    build_one "$1"
fi