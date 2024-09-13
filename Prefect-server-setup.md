# Getting the Prefect server up and running

## Setup the database
Make sure you have the PostgreSQL DB running using Docker Compose
```
docker-compose up --build --force-recreate --remove-orphans
```

## Setup and launch Prefect
Type the following commands on the command line with the envionment you installed Prefect in active
```
prefect server database reset -y
prefect config set PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://postgres:password@localhost:5432/prefect_server"
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
prefect config view --show-sources
prefect server start
```

## Code setup
Need to download `spacy` model that we will be using
python -m spacy download en_core_web_sm