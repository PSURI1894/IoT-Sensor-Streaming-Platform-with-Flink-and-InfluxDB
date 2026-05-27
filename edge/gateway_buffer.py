import time
import sqlite3
import json
import random
import paho.mqtt.client as mqtt

# Mimics an edge gateway buffering data in a local SQLite queue during WAN outages
class GatewayBuffer:
    def __init__(self, db_path="gateway_queue.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT, payload TEXT, created_at INTEGER)"
        )
        self.conn.commit()

    def enqueue(self, topic, payload):
        self.conn.execute(
            "INSERT INTO queue (topic, payload, created_at) VALUES (?, ?, ?)",
            (topic, json.dumps(payload), int(time.time() * 1000))
        )
        self.conn.commit()

    def dequeue_batch(self, batch_size=100):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, topic, payload FROM queue ORDER BY id ASC LIMIT ?", (batch_size,))
        rows = cursor.fetchall()
        return rows

    def remove_batch(self, ids):
        if ids:
            placeholders = ",".join("?" for _ in ids)
            self.conn.execute(f"DELETE FROM queue WHERE id IN ({placeholders})", ids)
            self.conn.commit()

    def size(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM queue")
        return cursor.fetchone()[0]

def main():
    buffer = GatewayBuffer()
    print("Edge gateway spooling initialized. Local buffer database ready.")
    
    # Simulate adding records and checking connectivity
    for i in range(10):
        buffer.enqueue("site/1/asset/pump-01/temperature", {"value": 55.4 + i, "event_ts": int(time.time()*1000)})
        
    print(f"Spool queue size: {buffer.size()} events buffered.")
    batch = buffer.dequeue_batch(5)
    print(f"Dequeued batch of {len(batch)} records for transmission.")
    ids_to_del = [row[0] for row in batch]
    buffer.remove_batch(ids_to_del)
    print(f"Buffer size after ack: {buffer.size()} events.")

if __name__ == "__main__":
    main()
