import json, time
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from random import uniform
consumer = KafkaConsumer(bootstrap_servers=['172.24.41.199:8084'],
	value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe(topics=("Update_Cupos_v2"))
conn = None
try:
    conn = psycopg2.connect(host="172.24.99.21",database="arquisicion", user="admin", password="admin123",port = "5432")
    print("Iniciado servicio de atualizacion de cupos")
    for message in consumer:
        print("id = %s  || cupos = %s" % (message.value['id'], message.value['cupos']))
        cur = conn.cursor()
        cur.execute("UPDATE parqueaderoempresaentity SET cupos = " + str(message.value['cupos']) + " WHERE id = " + str(message.value['id'])+ ";")
        conn.commit()
        cur.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
