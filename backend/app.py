import sqlite3

import pandas as pd
import requests
import streamlit as st

DB_PATH = "disaster.db"

st.set_page_config(page_title="災害リスク分析ダッシュボード", layout="wide")
st.title("災害リスク分析ダッシュボード v2")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS disaster (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT,
            region TEXT,
            type TEXT,
            magnitude REAL
        )
    """
    )
    conn.commit()
    conn.close()


def fetch_earthquake():
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data_json = res.json()
    except Exception as e:
        st.error(f"データ取得に失敗しました: {e}")
        return pd.DataFrame()

    data = []
    for f in data_json.get("features", []):
        props = f["properties"]

        data.append(
            {
                "time": pd.to_datetime(props["time"], unit="ms"),
                "region": props["place"],
                "type": "地震",
                "magnitude": props.get("mag", 0),
            }
        )

    return pd.DataFrame(data)


def save_to_db(df):
    if df.empty:
        return

    conn = sqlite3.connect(DB_PATH)

    for _, row in df.iterrows():
        conn.execute(
            """
            INSERT INTO disaster (time, region, type, magnitude)
            VALUES (?, ?, ?, ?)
        """,
            (str(row["time"]), row["region"], row["type"], row["magnitude"]),
        )

    conn.commit()
    conn.close()


def load_from_db():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM disaster", conn)
    conn.close()

    if not df.empty:
        df["time"] = pd.to_datetime(df["time"])

    return df


def calculate_risk(df):
    df = df.copy()

    now = pd.Timestamp.now()
    df["time_weight"] = 1 / (1 + (now - df["time"]).dt.days)

    df["risk_score"] = df["magnitude"] * df["time_weight"]

    return df


def main():
    init_db()

    col1, col2 = st.columns(2)

    with col1:
        if st.button("最新データを取得"):
            df_new = fetch_earthquake()
            save_to_db(df_new)
            st.success("データを更新しました")

    df = load_from_db()

    if df.empty:
        st.warning("データがありません。取得してください。")
        return

    df = calculate_risk(df)

    st.sidebar.header("フィルター")

    region_list = df["region"].unique()
    selected_region = st.sidebar.selectbox("地域", ["すべて"] + list(region_list))

    mag_min, mag_max = st.sidebar.slider(
        "マグニチュード",
        float(df["magnitude"].min()),
        float(df["magnitude"].max()),
        (float(df["magnitude"].min()), float(df["magnitude"].max())),
    )

    if selected_region != "すべて":
        df = df[df["region"] == selected_region]

    df = df[(df["magnitude"] >= mag_min) & (df["magnitude"] <= mag_max)]

    st.subheader("サマリー")

    col1, col2, col3 = st.columns(3)

    col1.metric("データ件数", len(df))
    col2.metric("最大マグニチュード", df["magnitude"].max())
    col3.metric("総リスクスコア", round(df["risk_score"].sum(), 2))

    st.subheader("地域別リスクランキング")

    ranking = df.groupby("region")["risk_score"].sum().sort_values(ascending=False)
    st.bar_chart(ranking.head(10))

    st.subheader("リスクの時系列変化")

    ts = df.groupby(df["time"].dt.date)["risk_score"].sum()
    st.line_chart(ts)

    st.subheader("データ詳細")
    st.dataframe(df)


if __name__ == "__main__":
    main()
