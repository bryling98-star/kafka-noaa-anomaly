# Kakfa NOAA Anomaly

Purpose: To play with Kafka and ingest some weather data, process it, and consume the processed data.

There is currently no data retention. So data in/out is not kept.

## How To:

It's a bit complex. But in a nutshell, you need Docker Desktop, GIT. Put this repo in a folder, then start up docker-compose up kafka (only). Apply the start-topics.sh manually into its bash via `docker exec it`. Then quit. docker-compose up the other 3 containers. Now it will tick. You'll have to watch the weather.raw and weather.processed streams to see data. But there isn't much more to it right now. And its a shoe-string deployment.

## Note:

NOAA producer is not live yet, so you can ignore that folder for now. I don't have it setup yet
