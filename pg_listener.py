# pg_listener.py
import select
import psycopg2
from kafka import KafkaProducer
import json

# PostgreSQL connection parameters (adjust as needed)
db_config = {
        "dbname": "postgres",
        "user": "postgres",
        "host": "localhost",
        "port": "5432"
    }
conn = psycopg2.connect(**db_config)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

# Set up a TCP socket server to send notifications to Spark
HOST = 'localhost'
PORT = 9999  # choose an available port


# Listen to the channel table_insert
cursor.execute("LISTEN table_insert;")
print("Listener is active and waiting for events...")

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: v.encode('utf-8')
)

try:
    while True:
        # Wait for notifications; timeout set to 5 seconds
        if select.select([conn], [], [], 5) == ([], [], []):
            continue
        else:
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                # Assume notify.payload is a JSON string with row_id and client_id
                payload = notify.payload
                print("Received notification:", payload)
                # Send only the payload to Kafka
                producer.send("notifications", value=payload)
                producer.flush()

except KeyboardInterrupt:
    print("Listener interrupted, shutting down.")
finally:
    cursor.close()
    conn.close()