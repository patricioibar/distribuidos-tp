#!/bin/bash

# Build script for Filter Service Docker image

set -e

echo "Building Filter Service Docker image..."

# Get the current directory name as image name
IMAGE_NAME="filter-service"
VERSION="latest"

# Build the Docker image
docker build -t ${IMAGE_NAME}:${VERSION} .

echo "✅ Docker image built successfully: ${IMAGE_NAME}:${VERSION}"

# Show image size
echo "📊 Image size:"
docker images ${IMAGE_NAME}:${VERSION} --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"

echo ""
echo "🚀 To run the container:"
echo "docker run --rm -e FILTER_ID=worker1 -e FILTER_TYPE=TbyYear [other env vars] ${IMAGE_NAME}:${VERSION}"
echo ""
echo "📝 Or use docker-compose:"
echo "docker-compose up"