
criar um topic no kafka:
  kafka-topics.sh --bootstrap-server kafka:9092 --topic topic-spark --create --partitions 1 --replication-factor 1
  se estive usando docker, entrar dentro do container--> docker exec -it kafka bash
