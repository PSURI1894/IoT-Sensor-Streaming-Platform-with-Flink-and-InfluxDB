import time
import json
import random
import paho.mqtt.client as mqtt

# Configuration
BROKER = "localhost"
PORT = 1883
TOPIC_FORMAT = "site/{site}/asset/{asset}/{metric}"
ASSETS = [
    {"id": "austin-pump-01", "site": "1", "class": "PUMP"},
    {"id": "austin-compressor-02", "site": "1", "class": "COMPRESSOR"},
    {"id": "berlin-turbine-03", "site": "2", "class": "TURBINE"}
]

def generate_telemetry(asset_class):
    base_temp = 55.0 if asset_class == "PUMP" else 75.0
    base_vib = 1.8 if asset_class == "PUMP" else 3.2
    
    # Add noise + potential anomaly
    anomaly_roll = random.random()
    is_anomaly = anomaly_roll > 0.98
    
    temp = base_temp + random.uniform(-2, 2)
    vib = base_vib + random.uniform(-0.3, 0.3)
    pressure = random.uniform(40, 90)
    rpm = random.uniform(1000, 3000)
    
    if is_anomaly:
        temp += random.uniform(20, 45) # Trigger thresholds
        vib += random.uniform(2.5, 4.0)
        
    return {
        "temperature": round(temp, 2),
        "vibration": round(vib, 2),
        "pressure": round(pressure, 2),
        "rpm": round(rpm, 2)
    }

def main():
    client = mqtt.Client("Simulator_Edge_Client")
    try:
        client.connect(BROKER, PORT, 60)
        print(f"Connected to EMQX Broker on {BROKER}:{PORT}")
    except Exception as e:
        print(f"Failed to connect: {e}")
        return

    client.loop_start()
    
    seq = 0
    try:
        while True:
            for asset in ASSETS:
                telemetries = generate_telemetry(asset["class"])
                timestamp = int(time.time() * 1000)
                
                for metric, value in telemetries.items():
                    topic = TOPIC_FORMAT.format(site=asset["site"], asset=asset["id"], metric=metric)
                    payload = {
                        "event_id": f"{asset['id']}_{metric}_{seq}",
                        "asset_id": asset["id"],
                        "device_class": asset["class"],
                        "metric_name": metric,
                        "value": value,
                        "event_ts": timestamp,
                        "quality_flag": 1
                    }
                    client.publish(topic, json.dumps(payload), qos=1)
            seq += 1
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("Stopping simulator...")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
