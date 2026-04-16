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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_hour TEXT NOT NULL,
        hour INTEGER NOT NULL,
        region TEXT NOT NULL,
        event_type TEXT NOT NULL,
        intensity INTEGER NOT NULL,
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
                    (snapshot_hour, hour, region, event_type, intensity)
                    VALUES (?, ?, ?, ?, ?)
                    """, (
                        now.isoformat(),
                        now.hour,
                        region,
                        warning.get("code"),
                        1
                    ))

    conn.commit()
    conn.close()

if __name__ == "__main__":
    data = fetch()
    save(data)