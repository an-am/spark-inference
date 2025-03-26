# pg_listener.py
import select
import psycopg2
import socket

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

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(1)
print(f"Socket server listening on {HOST}:{PORT}")

client_socket, addr = server_socket.accept()
print(f"Accepted connection from {addr}")

# Listen to the channel table_insert
cursor.execute("LISTEN table_insert;")
print("Listener is active and waiting for events...")

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
                # print("Received notification:", payload)
                # Send the payload over the socket, ending with a newline
                client_socket.sendall((payload + "\n").encode("utf-8"))
except KeyboardInterrupt:
    print("Listener interrupted, shutting down.")
finally:
    client_socket.close()
    server_socket.close()
    cursor.close()
    conn.close()