import json
import logging
from fastapi import FastAPI
import aioredis
from aiokafka import AIOKafkaConsumer

app = FastAPI(title="Industrial Telemetry Deduplicating Alert Service")
REDIS_URL = "redis://localhost:6379"
KAFKA_BOOTSTRAP = "localhost:9092"
ALERTS_TOPIC = "alerts.raw"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

redis = None

@app.on_event("startup")
async def startup_event():
    global redis
    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    logger.info("Connected to Redis cache.")
    
    # Run async background Kafka Alert consumer
    import asyncio
    asyncio.create_task(consume_alerts())

async def consume_alerts():
    consumer = AIOKafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="alert-service-group"
    )
    await consumer.start()
    logger.info("Alert Consumer started. Listening on alerts.raw topic...")
    try:
        async for msg in consumer:
            alert = json.loads(msg.value.decode('utf-8'))
            await process_alert(alert)
    finally:
        await consumer.stop()

async def process_alert(alert):
    asset_id = alert["assetId"]
    rule_id = alert["ruleId"]
    val = alert["triggeredValue"]
    
    # 5-minute Sliding Alert-Storm Deduplication key via Redis SETNX
    dedup_key = f"alert:{asset_id}:{rule_id}:dedup"
    is_new = await redis.set(dedup_key, "active", ex=300, nx=True)
    
    if is_new:
        logger.info(f"[TRIGGER ALERT] Machine failure risk on {asset_id}! Code: {rule_id}. Trigger value: {val}.")
        # Mock PagerDuty trigger routing
    else:
        # Increment suppressed audit key
        await redis.incr(f"alert:{asset_id}:{rule_id}:suppressed_count")
        logger.debug(f"[SUPPRESSED] Duplicate alert blocked for {asset_id} under rule {rule_id}.")

@app.get("/health")
def health():
    return {"status": "healthy", "service": "alert-service"}
