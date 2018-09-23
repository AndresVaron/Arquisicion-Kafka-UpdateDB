import json, time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from random import uniform

producer = KafkaProducer(bootstrap_servers=['172.24.41.199:8084'],
						 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
	producer.send('Update_Cupos', {'id': '1', 'tipo':'salida'})
	producer.flush()
	time.sleep(2)
