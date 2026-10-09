"""Create the Kafka topic used by the book-processing lab."""
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "book-lines"


def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=10)
    if TOPIC in metadata.topics and metadata.topics[TOPIC].error is None:
        print(f"Topic '{TOPIC}' already exists.")
        return
    results = admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
    for topic, future in results.items():
        try:
            future.result(timeout=15)
            print(f"Topic '{topic}' created successfully.")
        except KafkaException as exc:
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                print(f"Topic '{topic}' already exists.")
            else:
                raise


if __name__ == "__main__":
    main()
