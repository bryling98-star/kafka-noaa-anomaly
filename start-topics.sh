/opt/kafka/bin/kafka-topics.sh --create --topic weather.raw --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092
/opt/kafka/bin/kafka-topics.sh --create --topic weather.alert --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092
/opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic weather.processed \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server kafka:9092
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092