import json, time
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from random import uniform
consumer = KafkaConsumer(bootstrap_servers=['ec2-35-161-3-13.us-west-2.compute.amazonaws.com:8081'],
	value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe(topics=("PAGOS"))
conn = None
try:
    conn = psycopg2.connect()
    print("Iniciado servicio de pagos")
    for message in consumer:
        print("id = %s  || tipo = %s" % (message.value['id'], message.value['tipo']))
        if message.value['tipo'] == 'OnDemand':
            cur = conn.cursor()
            print("Pagando...")
            cur.execute("UPDATE  contratoondemandentity SET estado = 0 WHERE id = " + str(message.value['id'])+ ";")
            conn.commit()
            cur.close()
            print("Pago realizado")
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
