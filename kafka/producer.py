import csv
import json
import os
import random
import signal
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

from exchange_rate import ExchangeRateError, VietcombankExchangeRate, convert_usd_to_vnd


def env_float(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value < 0:
        raise ValueError(f"{name} must be greater than or equal to 0")
    return value


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


CSV_PATH = Path(os.getenv("CSV_PATH", "User0_credit_card_transactions.csv"))
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "credit-card-transactions")
MIN_DELAY_SECONDS = env_float("MIN_DELAY_SECONDS", 1.0)
MAX_DELAY_SECONDS = env_float("MAX_DELAY_SECONDS", 5.0)
LOOP = env_bool("LOOP", False)

if MAX_DELAY_SECONDS < MIN_DELAY_SECONDS:
    raise ValueError("MAX_DELAY_SECONDS must be greater than or equal to MIN_DELAY_SECONDS")

if not CSV_PATH.is_absolute():
    CSV_PATH = Path(__file__).resolve().parent.parent / CSV_PATH

running = True


def stop(_signum, _frame):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def send_rows(producer: KafkaProducer) -> int:
    sent = 0
    exchange_rate = VietcombankExchangeRate()

    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            raise ValueError(f"CSV file has no header: {CSV_PATH}")

        for row_number, row in enumerate(reader, start=2):
            if not running:
                break

            try:
                rate, rate_source, published_at = exchange_rate.get_usd_vnd()
                amount_vnd = convert_usd_to_vnd(row.get("Amount", "0"), rate)
                row["Exchange_Rate_USD_VND"] = str(rate)
                row["Amount_VND"] = str(amount_vnd)
                row["Exchange_Rate_Source"] = rate_source
                row["Exchange_Rate_Date"] = published_at
            except (ExchangeRateError, ValueError) as error:
                print(f"Failed to convert row {row_number}: {error}", file=sys.stderr)
                raise

            future = producer.send(
                TOPIC,
                key=str(row.get("Card", "")).encode("utf-8"),
                value=row,
            )

            try:
                future.get(timeout=10)
            except KafkaError as error:
                print(f"Failed to send CSV row {row_number}: {error}", file=sys.stderr)
                raise

            sent += 1
            print(f"Sent row {row_number}: card={row.get('Card')} topic={TOPIC}")

            if running:
                delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
                time.sleep(delay)

    return sent


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(
        value,
        ensure_ascii=False,
    ).encode("utf-8"),
    key_serializer=lambda key: key,
    acks="all",
    retries=5,
)

try:
    total_sent = 0
    while running:
        total_sent += send_rows(producer)
        if not LOOP:
            break

    print(f"Finished. Sent {total_sent} transaction(s).")
finally:
    producer.flush(timeout=10)
    producer.close(timeout=10)
