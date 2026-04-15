import sqlite3

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import pandas as pd
import requests
import streamlit as st

font_path = r"C:\Users\lunat\OneDrive\ドキュメント\Python\Noto_Sans_JP\NotoSansJP-VariableFont_wght.ttf"
font_prop = fm.FontProperties(fname=font_path)

plt.rcParams["font.family"] = font_prop.get_name()


# ======================
# 地域コード → 都道府県（日本語）
# ======================
def convert_region(code):
    code = str(code)

    mapping = {
        "01": "北海道",
        "02": "青森県",
        "03": "岩手県",
        "04": "宮城県",
        "05": "秋田県",
        "06": "山形県",
        "07": "福島県",
        "08": "茨城県",
        "09": "栃木県",
        "10": "群馬県",
        "11": "埼玉県",
        "12": "千葉県",
        "13": "東京都",
        "14": "神奈川県",
        "15": "新潟県",
        "16": "富山県",
        "17": "石川県",
        "18": "福井県",
        "19": "山梨県",
        "20": "長野県",
        "21": "岐阜県",
        "22": "静岡県",
        "23": "愛知県",
        "24": "三重県",
        "25": "滋賀県",
        "26": "京都府",
        "27": "大阪府",
        "28": "兵庫県",
        "29": "奈良県",
        "30": "和歌山県",
        "31": "鳥取県",
        "32": "島根県",
        "33": "岡山県",
        "34": "広島県",
        "35": "山口県",
        "36": "徳島県",
        "37": "香川県",
        "38": "愛媛県",
        "39": "高知県",
        "40": "福岡県",
        "41": "佐賀県",
        "42": "長崎県",
        "43": "熊本県",
        "44": "大分県",
        "45": "宮崎県",
        "46": "鹿児島県",
        "47": "沖縄県",
    }

    return mapping.get(code[:2], "不明")


# ======================
# 災害種別変換（日本語）
# ======================
def convert_event(code):
    mapping = {
        "14": "大雨",
        "15": "洪水",
        "16": "暴風",
        "17": "大雪",
        "21": "強風",
    }
    return mapping.get(str(code), "その他")


# ======================
# データ取得（気象庁）
# ======================
def fetch_data():
    url = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"

    res = requests.get(url)
    data = res.json()

    records = []

    for report in data:
        try:
            dt = pd.to_datetime(report["reportDatetime"])

            for area_type in report.get("areaTypes", []):
                for area in area_type.get("areas", []):
                    region = convert_region(area.get("code"))

                    for warning in area.get("warnings", []):
                        status = warning.get("status")

                        # 解除は除外
                        if status == "解除":
                            continue

                        records.append(
                            {
                                "datetime": dt,
                                "date": dt.date(),
                                "hour": dt.hour,
                                "region": region,
                                "event_type": convert_event(warning.get("code")),
                                "intensity": 1,
                            }
                        )

        except:
            continue

    return pd.DataFrame(records)


# ======================
# DB保存
# ======================
def save_to_db(df):
    if df.empty:
        return

    conn = sqlite3.connect("disaster.db")
    df.to_sql("disaster", conn, if_exists="append", index=False)
    conn.close()


# ======================
# データ読み込み
# ======================
def load_data():
    conn = sqlite3.connect("disaster.db")

    try:
        df = pd.read_sql("SELECT * FROM disaster", conn)
    except:
        df = pd.DataFrame()

    conn.close()
    return df


# ======================
# リスク計算
# ======================
def calculate_risk(df):
    freq = df.groupby("region").size()
    return freq.sort_values(ascending=False)


# ======================
# グラフ
# ======================
def plot_bar(data, title):
    fig, ax = plt.subplots()

    data.plot(kind="bar", ax=ax)

    # 🔥 日本語フォントを強制適用
    ax.set_title(title, fontproperties=font_prop)
    ax.set_xlabel("地域", fontproperties=font_prop)
    ax.set_ylabel("件数", fontproperties=font_prop)

    # X軸ラベルも日本語対応
    for label in ax.get_xticklabels():
        label.set_fontproperties(font_prop)

    plt.xticks(rotation=0)
    plt.tight_layout()

    st.pyplot(fig)


# ======================
# UI
# ======================
st.title("災害リスク分析ツール（日本）")

st.write(
    """
気象庁の警報データを使用し、
地域別・時間帯別の発生傾向を可視化しています。
"""
)

# データ取得
if st.button("データ取得＆保存"):
    df_new = fetch_data()
    st.write("取得件数:", len(df_new))
    save_to_db(df_new)

# データ読み込み
df = load_data()

if df.empty:
    st.warning("データがありません")
else:
    # 🔥 最新500件に制限
    df = df.tail(500)

    st.write("総データ数:", len(df))

    # 地域別
    st.subheader("地域別の災害発生数（上位10件）")
    region_counts = df.groupby("region").size().sort_values(ascending=False).head(10)
    plot_bar(region_counts, "地域別発生数")

    # 時間帯別
    st.subheader("時間帯別の発生数")
    hour_counts = df.groupby("hour").size()
    plot_bar(hour_counts, "時間帯別発生数")

    # リスク
    st.subheader("リスクランキング（発生頻度ベース）")
    risk = calculate_risk(df)
    st.write(risk.head(10))

    top_region = risk.index[0]
    st.write(f"最もリスクが高い地域: {top_region}")
