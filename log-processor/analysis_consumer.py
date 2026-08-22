import json
import time
from kafka import KafkaConsumer

def main():
    print("Analysis consumer started")
    
    consumer = None
    while not consumer:
        try:
            consumer = KafkaConsumer(
                "logs",
                bootstrap_servers=["kafka:9094"],
                group_id="analysis-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode("utf-8"))
            )
        except Exception as e:
            print(f"Waiting for Kafka to be ready: {e}")
            time.sleep(3)
            
    print("Successfully connected to Kafka. Waiting for logs...")

    # Simple metrics aggregation
    metrics = {
        "total_logs": 0,
        "errors": 0,
        "warnings": 0,
        "services": {}
    }

    last_report_time = time.time()
    REPORT_INTERVAL = 10  # Print metrics every 10 seconds

    for message in consumer:
        log = message.value
        
        if not isinstance(log, dict) or not log.get("service"):
            continue

        service = log["service"]
        level = log.get("level", "").lower()

        # Update metrics
        metrics["total_logs"] += 1
        
        if level == "error":
            metrics["errors"] += 1
            print(f"[ALERT] Error detected in {service}: {log.get('message', 'No message')}")
        elif level == "warning":
            metrics["warnings"] += 1

        if service not in metrics["services"]:
            metrics["services"][service] = 0
        metrics["services"][service] += 1

        # Print report periodically
        current_time = time.time()
        if current_time - last_report_time >= REPORT_INTERVAL:
            print("\n--- Real-Time Analysis Report ---")
            print(f"Total Logs Analyzed: {metrics['total_logs']}")
            print(f"Total Errors: {metrics['errors']}")
            print(f"Total Warnings: {metrics['warnings']}")
            print("Logs per Service:")
            for srv, count in metrics["services"].items():
                print(f"  - {srv}: {count}")
            print("---------------------------------\n")
            
            last_report_time = current_time

if __name__ == "__main__":
    main()
