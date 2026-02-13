from kafka import KafkaConsumer
import json
import mysql.connector
import uuid
import time

# Define the Kafka broker and topic
broker = 'my-kafka.kylianvkr-dev.svc.cluster.local:9092'
topic = 'dbserver1.customer'

# Definie the database connection parameters
db_host = 'eurynome-db'
db_port = 3306
db_user = 'eurynomeuser'
db_password = 'eurynomepw'
db_name = ''

# Create a Kafka consumer
try:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[broker],
        sasl_mechanism='SCRAM-SHA-256',
        security_protocol='SASL_PLAINTEXT',
        sasl_plain_username='user1',
        sasl_plain_password='Kvowl8m2SI',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='crm'
    )
    print(f"Connected to Kafka broker at {broker}")
    print(f"Listening to topic {topic}")
except Exception as e:
    print(f"Error connecting to Kafka broker: {e}")
    exit(1)

print(f"Connection to DB server at {db_host}:{db_port}")
# connect to a MySQL/MariaDB database
try:
    conn = mysql.connector.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database=db_name
    )
    print("Connected to MariaDB database")
except mysql.connector.Error as e:
    print(f"Error connecting to MariaDB database: {e}")
    exit(1)

# Poll messages from the Kafka topic
while True:
    for message in consumer:
        print(f"Received message: {message.value}")