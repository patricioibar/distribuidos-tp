docker build -f ./monitor/Dockerfile -t monitor:latest .
docker build -f ./filter/Dockerfile -t filter:latest .
docker build -f ./aggregator/Dockerfile -t aggregator:latest .
docker build -f ./joiner/Dockerfile -t joiner:latest .
docker build -f ./coffee-analyzer/Dockerfile -t coffee-analyzer:latest .
docker build -f ./analyst/Dockerfile -t analyst:latest .