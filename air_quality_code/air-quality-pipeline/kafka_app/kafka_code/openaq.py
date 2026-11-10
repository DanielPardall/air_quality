import requests
import time

API_KEY = "YOUR_OPENAQ_API_KEY"
BASE = "https://api.openaq.org/v3"
HEADERS = {"X-API-Key": API_KEY}

def fetch_new(sensor_id: int, since_utc: str, max_retries: int = 3):

    page = 1

    while True:
        
        for attempt in range(max_retries):
            try:
                r = requests.get(
                    f"{BASE}/sensors/{sensor_id}/measurements",
                    headers=HEADERS,
                    params={
                        "datetime_from": since_utc,
                        "limit": 1000,
                        "page": page,
                    },
                    timeout=30,
                )
                r.raise_for_status()
                break  
            except requests.exceptions.HTTPError as e:
                if r.status_code == 408 and attempt < max_retries - 1:
                    wait = 2 * (attempt + 1)
                    print(f"[WARN] Timeout 408 sensor {sensor_id} page {page}, retry em {wait}s...")
                    time.sleep(wait)
                    continue
                raise
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait = 2 * (attempt + 1)
                    print(f"[WARN] Erro de rede sensor {sensor_id} page {page}: {e}. Retry em {wait}s...")
                    time.sleep(wait)
                    continue
                raise

        data = r.json()
        results = data.get("results", [])
        if not results:
            break

        results.sort(
            key=lambda m: (
                (m.get("period") or {})
                .get("datetimeTo", {})
                .get("utc", "")
            )
        )

        for m in results:
            period = m.get("period") or {}
            dt_to = (period.get("datetimeTo") or {})
            dt_utc = dt_to.get("utc") or ""

            if dt_utc and dt_utc > since_utc:
                yield m

        page += 1
        time.sleep(0.3)
