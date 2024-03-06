# Remove any running services
./stop.sh

# Check if the network exists
if ! docker network ls | grep -q superagent_network; then
  # Create the network if it does not exist
  docker network create superagent_network
fi
# docker compose build --no-cache
# Run the db services
docker compose \
        --env-file .env \
        up \
        --build \
        -d
        
docker logs super-rag -f