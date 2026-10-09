"""Send each line from a UTF-8 book to Kafka, followed by an end marker."""
from pathlib import Path
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "book-lines"
BOOK = Path(__file__).with_name("book.txt")
END_MARKER = "__KAFKA_LAB_END_OF_BOOK_9f2a__"


def delivery_report(error, message):
    if error:
        print(f"Delivery failed: {error}")


def main():
    if not BOOK.exists():
        raise FileNotFoundError(f"Missing {BOOK}. Download a .txt book and rename it book.txt")
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    count = 0
    with BOOK.open("r", encoding="utf-8-sig", errors="replace") as handle:
        for line in handle:
            producer.produce(TOPIC, value=line.rstrip("\r\n").encode("utf-8"), callback=delivery_report)
            producer.poll(0)
            count += 1
    producer.produce(TOPIC, value=END_MARKER.encode("utf-8"), callback=delivery_report)
    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"{remaining} messages not delivered")
    print(f"Sent {count} book lines to topic '{TOPIC}' and an end marker.")


if __name__ == "__main__":
    main()
