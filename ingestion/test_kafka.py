from kafka import KafkaProducer
p = KafkaProducer(bootstrap_servers="localhost:9092", request_timeout_ms=10000)
f = p.send("releves", b"test-python")
print("Résultat :", f.get(timeout=10))