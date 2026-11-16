import paho.mqtt.client as mqtt
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="127.0.0.1:9092")

def on_connect(client, userdata, flags, reason_code, properties):
    print("Connecté à MQTT, code :", reason_code)
    client.subscribe("releves/#")

def on_subscribe(client, userdata, mid, reason_codes, properties):
    print("Abonnement confirmé :", reason_codes)

def on_message(client, userdata, msg):
    print("Reçu de MQTT :", msg.topic, msg.payload[:60])
    key = msg.topic.split("/")[-1].encode()
    try:
        meta = producer.send("releves", key=key, value=msg.payload).get(timeout=10)
        print("Transmis -> partition", meta.partition, "offset", meta.offset)
    except Exception as e:
        print("ERREUR Kafka :", repr(e))

c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
c.on_connect = on_connect
c.on_subscribe = on_subscribe
c.on_message = on_message
c.connect("127.0.0.1", 1883)
print("Passerelle en écoute...")
c.loop_forever()