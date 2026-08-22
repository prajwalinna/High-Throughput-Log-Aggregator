import json
import logging
import os
import threading
import time
from kafka import KafkaProducer

class KafkaFallbackHandler(logging.Handler):
    def __init__(self, service_name, broker_url="localhost:9092", topic="logs"):
        super().__init__()
        self.service_name = service_name
        self.broker_url = broker_url
        self.topic = topic
        
        # Setup fallback file
        log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        self.fallback_file = os.path.join(log_dir, f"{service_name}_fallback.log")
        
        self.producer = None
        self._init_producer()
        
        # Start a background thread to retry sending fallback logs
        self.retry_thread = threading.Thread(target=self._retry_fallback_loop, daemon=True)
        self.retry_thread.start()

    def _init_producer(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.broker_url],
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            return True
        except Exception as e:
            self.producer = None
            return False

    def emit(self, record):
        log_entry = self.format(record)
        
        # Attempt to parse back if formatter returns string, to keep JSON pure
        if isinstance(log_entry, str):
            try:
                log_entry = json.loads(log_entry)
            except:
                pass

        if self.producer:
            try:
                self.producer.send(self.topic, log_entry)
                self.producer.flush()
                return
            except Exception as e:
                # Producer failed (e.g. Kafka went down), switch to fallback
                self.producer = None

        # Fallback to local file
        self._write_to_fallback(log_entry)

    def _write_to_fallback(self, log_entry):
        try:
            with open(self.fallback_file, "a", encoding="utf-8") as f:
                if isinstance(log_entry, dict):
                    f.write(json.dumps(log_entry) + "\n")
                else:
                    f.write(str(log_entry) + "\n")
        except Exception:
            pass

    def _retry_fallback_loop(self):
        while True:
            time.sleep(5) # Check every 5 seconds
            
            if not os.path.exists(self.fallback_file) or os.path.getsize(self.fallback_file) == 0:
                continue

            if not self.producer:
                if not self._init_producer():
                    continue

            # Kafka is up, try to read fallback file
            try:
                with open(self.fallback_file, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                
                if not lines:
                    continue

                for line in lines:
                    if not line.strip():
                        continue
                    log_entry = json.loads(line)
                    self.producer.send(self.topic, log_entry)
                
                self.producer.flush()
                
                # If successful, delete the file contents
                with open(self.fallback_file, "w", encoding="utf-8") as f:
                    f.truncate(0)
                    
            except Exception as e:
                # Failed to process fallback, kafka might have gone down again
                self.producer = None
