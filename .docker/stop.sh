# Remove any running services
docker compose \
        down \
        --remove-orphans
        # -v # TODO: remove the -v flag when we have a persistent database
