import requests
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

DB_PATH = "disaster.db"

def fetch():
    url = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"
    res = requests.get(url)
    res.raise_for_status()
    return res.json()

def save(data):
    conn = sqlite3.connect(DB_PATH)

    now = datetime.now(ZoneInfo("Asia/Tokyo")).replace(
        minute=0, second=0, microsecond=0
    )

    conn.execute("""
    CREATE TABLE IF NOT EXISTS disasters (
        snapshot_hour TEXT,
        region TEXT,
        event_type TEXT,
        UNIQUE(snapshot_hour, region, event_type)
    )
    """)

    for report in data:
        for area_type in report.get("areaTypes", []):
            for area in area_type.get("areas", []):
                region = str(area.get("code"))[:2]

                for warning in area.get("warnings", []):
                    if warning.get("status") == "解除":
                        continue

                    conn.execute("""
                    INSERT OR IGNORE INTO disasters
                    VALUES (?, ?, ?)
                    """, (now.isoformat(), region, warning.get("code")))

    conn.commit()
    conn.close()

if __name__ == "__main__":
    data = fetch()
    save(data)
