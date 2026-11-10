from openaq import fetch_new
from kafka import KafkaProducer
import json
import time

# ID da localização Olivais (Lisboa)
LOCATION_ID = 8036

# Sensores da estação Olivais - excluindo PM10/PM25
SENSORS = [
    5078482,  # co
    5078758,  # no2
    5079040,  # o3
]


START = "2025-01-01T00:00:00+00:00"

# Configuração Kafka
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
)

last_seen = {sid: START for sid in SENSORS}

print("Producer iniciado.")


while True:
    for sid in SENSORS:
        since = last_seen[sid]
        new_last = since
        count = 0

        for m in fetch_new(sid, since):
            period = m.get("period") or {}
            dt_to = (period.get("datetimeTo") or {})
            dt_utc = dt_to.get("utc") or dt_to.get("local")

            param = m["parameter"]

            event = {
                "sensor_id": sid,
                "location_id": LOCATION_ID,
                "parameter": param["name"],   
                "unit": param["units"],       
                "value": m["value"],
                "datetime_utc": dt_utc,       
            }

            producer.send("air_quality", key=sid, value=event)
            new_last = dt_utc
            count += 1

        last_seen[sid] = new_last
        print(f"Sensor {sid}: {count} medições novas enviadas.")

    producer.flush()
    time.sleep(60)
