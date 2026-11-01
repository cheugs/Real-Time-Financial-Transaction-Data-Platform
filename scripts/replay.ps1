# Replay: wipe Spark's checkpoints and the output tables, then let Spark
# re-read the whole Kafka topic from the beginning.
# Run from the project folder:  .\scripts\replay.ps1
$ErrorActionPreference = "Stop"

Write-Host "Stopping Spark..."
docker compose stop spark

Write-Host "Clearing output tables..."
$sql = "TRUNCATE raw_transactions, bad_events, txn_volume_per_minute, batch_log; " +
       "INSERT INTO pipeline_events (event_type, details) VALUES ('replay_requested', 'tables and checkpoints cleared');"
$sql | docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U $POSTGRES_USER -d $POSTGRES_DB'

Write-Host "Deleting Spark checkpoints..."
docker compose run --rm --no-deps --entrypoint sh spark -c "rm -rf /opt/checkpoints/*"

Write-Host "Starting Spark again (it will read Kafka from the earliest offset)..."
docker compose up -d spark
