import json
import time
import requests
from kafka import KafkaConsumer

LOKI_URL = "http://loki:3100/loki/api/v1/push"
BATCH_SIZE = 10  # Reduced for testing, increase for prod

def push_to_loki(logs):
    streams = []
    
    # Group logs by service
    service_logs = {}
    for log in logs:
        service = log.get("service", "unknown")
        if service not in service_logs:
            service_logs[service] = []
        
        # Loki requires timestamp in nanoseconds as string
        ts_ns = str(int(time.time() * 1e9))
        service_logs[service].append([ts_ns, json.dumps(log)])

    for service, values in service_logs.items():
        streams.append({
            "stream": {
                "service": service,
                "consumer": "storage"
            },
            "values": values
        })

    payload = {"streams": streams}

    try:
        response = requests.post(
            LOKI_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        response.raise_for_status()
        print(f"Successfully pushed {len(logs)} logs to Loki.")
    except Exception as e:
        print(f"Failed to push to Loki: {e}")
        # In production, implement retry logic here
        pass

def main():
    print("Storage consumer started")
    
    consumer = None
    while not consumer:
        try:
            consumer = KafkaConsumer(
                "logs",
                bootstrap_servers=["kafka:9094"],
                group_id="storage-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode("utf-8"))
            )
        except Exception as e:
            print(f"Waiting for Kafka to be ready: {e}")
            time.sleep(3)
            
    print("Successfully connected to Kafka. Waiting for logs...")

    batch = []
    for message in consumer:
        log = message.value
        
        if not isinstance(log, dict) or not log.get("service"):
            print("Skipping malformed log")
            continue

        batch.append(log)
        
        if len(batch) >= BATCH_SIZE:
            push_to_loki(batch)
            batch.clear()

if __name__ == "__main__":
    main()
