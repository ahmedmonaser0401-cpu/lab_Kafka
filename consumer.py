"""Read book lines from Kafka, clean their text, and write the result."""
import re
import unicodedata
from pathlib import Path
from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "book-lines"
OUTPUT = Path(__file__).with_name("cleaned_book.txt")
END_MARKER = "__KAFKA_LAB_END_OF_BOOK_9f2a__"


def clean_text(text):
    text = text.lower().strip()
    # Keep Unicode letters and numbers, replace punctuation/symbols with spaces.
    text = "".join(char if char.isspace() or char.isalnum() else " " for char in text)
    return " ".join(text.split())


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "book-lab-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    count = 0
    try:
        with OUTPUT.open("w", encoding="utf-8") as output:
            print(f"Reading topic '{TOPIC}' ... (Ctrl+C to stop)")
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                value = msg.value().decode("utf-8", errors="replace")
                if value == END_MARKER:
                    consumer.commit(asynchronous=False)
                    break
                cleaned = clean_text(value)
                if cleaned:
                    output.write(cleaned + "\n")
                    count += 1
                consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        print("Stopped by user; partial output saved.")
    finally:
        consumer.close()
    print(f"Saved {count} cleaned lines to {OUTPUT.name}")


if __name__ == "__main__":
    main()
