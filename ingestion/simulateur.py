import pandas as pd, json, time, random, sys
import paho.mqtt.client as mqtt

DELAI = float(sys.argv[1]) if len(sys.argv) > 1 else 0.5   # secondes entre 2 relevés
NB_COMPTEURS = 20

df = pd.read_csv("data/household_power_consumption.txt", sep=";",
                 na_values="?", nrows=200000).dropna()
df = df.iloc[::30]                     # 1 relevé toutes les 30 min

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect("localhost", 1883)
client.loop_start()

compteurs = [f"M{i:03d}" for i in range(1, NB_COMPTEURS + 1)]
n = 0
for _, row in df.iterrows():
    for m in compteurs:
        puissance = float(row.Global_active_power) * random.uniform(0.9, 1.1)
        anomalie = 0
        if random.random() < 0.002:          # anomalie volontaire (~0,2 %)
            puissance *= 5
            anomalie = 1
        msg = {
            "meter_id": m,
            "quartier": f"Q{int(m[1:]) % 4 + 1}",
            "timestamp": f"{row.Date} {row.Time}",
            "active_power": round(puissance, 3),
            "voltage": float(row.Voltage),
            "intensity": float(row.Global_intensity),
            "is_anomaly": anomalie,
        }
        client.publish(f"releves/{m}", json.dumps(msg))
        n += 1
    if n % 200 == 0:
        print(n, "relevés envoyés")
    time.sleep(DELAI)