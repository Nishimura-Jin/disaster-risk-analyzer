import random
import sqlite3
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

DB_PATH = "disaster.db"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS disaster (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        time TEXT,
        region TEXT,
        type TEXT
    )
    """
    )
    conn.commit()
    conn.close()


def generate_data(n=100):
    regions = ["大阪", "東京", "福岡"]
    types = ["地震", "警報", "避難"]

    data = []
    for _ in range(n):
        data.append(
            {
                "time": datetime.now() - timedelta(hours=random.randint(0, 100)),
                "region": random.choice(regions),
                "type": random.choice(types),
            }
        )

    return pd.DataFrame(data)


def save_to_db(df):
    conn = sqlite3.connect(DB_PATH)
    for _, row in df.iterrows():
        conn.execute(
            "INSERT INTO disaster (time, region, type) VALUES (?, ?, ?)",
            (str(row["time"]), row["region"], row["type"]),
        )
    conn.commit()
    conn.close()


def load_from_db():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM disaster", conn)
    conn.close()
    df["time"] = pd.to_datetime(df["time"])
    return df


def main():
    st.title("災害リスク分析ツール")

    init_db()

    if st.button("データ生成・保存"):
        df = generate_data()
        save_to_db(df)
        st.success("データを保存しました")

    df = load_from_db()

    if len(df) == 0:
        st.warning("データがありません")
        return

    st.subheader("地域別")
    st.bar_chart(df["region"].value_counts())

    st.subheader("種類別")
    st.bar_chart(df["type"].value_counts())

    df["hour"] = df["time"].dt.hour
    st.subheader("時間帯別")
    st.bar_chart(df["hour"].value_counts().sort_index())

    st.subheader("リスクランキング")
    ranking = df.groupby("region").size().sort_values(ascending=False)
    st.write(ranking)


if __name__ == "__main__":
    main()
