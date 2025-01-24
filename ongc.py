import json
import paho.mqtt.client as mqtt
import pyodbc
from datetime import datetime, timezone, timedelta
import time

# Function to convert epoch time to IST
def convert_epoch_to_ist(epoch_time):
    try:
        utc_dt = datetime.fromtimestamp(epoch_time, tz=timezone.utc)  # Convert epoch to UTC datetime
        ist_dt = utc_dt.astimezone(timezone(timedelta(hours=5, minutes=30)))  # Convert UTC to IST
        return ist_dt.strftime('%Y-%m-%d %H:%M:%S')  # Return formatted IST time as a string
    except Exception as e:
        print("Error converting epoch to IST:", e)
        return None

# Function to create a database connection with retry logic
def create_connection(connection_string):
    try:
        return pyodbc.connect(connection_string)
    except pyodbc.Error as e:
        print(f"Failed to connect to database: {e}")
        return None

# Class to manage a single database connection with retry logic
class ConnectionManager:
    def __init__(self, connection_string, retry_interval=60):
        self.connection_string = connection_string
        self.connection = None
        self.retry_interval = retry_interval

    def get_connection(self):
        if not self.connection:
            self.connection = create_connection(self.connection_string)
        return self.connection

    def close_connection(self):
        if self.connection:
            try:
                self.connection.close()
            except pyodbc.Error as e:
                print("Error closing SQL connection:", e)

# Callback function to handle incoming MQTT messages
def on_message(client, userdata, message):
    try:
        topic = message.topic
        payload = message.payload.decode('utf-8')

        # Parse the JSON message
        try:
            data = json.loads(payload)
            for item in data:
                try:
                    timestamp = item.get('t')
                    value = item.get('r')

                    # Split the topic to extract dataTagId (second last part of the topic)
                    topic_parts = topic.split('/')
                    if len(topic_parts) > 1:
                        data_tag_id = topic_parts[-2]  # Extract the second last part of the topic dynamically

                        # Convert timestamp to IST
                        ist_time = convert_epoch_to_ist(timestamp / 1000)  # Assuming timestamp is in milliseconds
                        if ist_time is None:
                            continue  # Skip if conversion fails

                        # Handle database connections
                        for db_name, db_manager in db_connections.items():
                            conn = db_manager.get_connection()
                            if conn:
                                try:
                                    cursor = conn.cursor()

                                    # Create the table if it doesn't exist
                                    create_table_query = f"""
                                    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ONGCServer1' AND xtype='U')
                                    CREATE TABLE ONGCServer1 (
                                        dataTagId NVARCHAR(50),
                                        value FLOAT,
                                        timestamp DATETIME
                                    );
                                    """
                                    cursor.execute(create_table_query)

                                    # Insert the data into the table
                                    insert_query = "INSERT INTO ONGCServer1 (dataTagId, value, timestamp) VALUES (?, ?, ?);"
                                    cursor.execute(insert_query, (data_tag_id, value, ist_time))
                                    conn.commit()  # Commit the transaction

                                    print(f"{db_name}: Inserted - DataTagId: {data_tag_id}, Value: {value}, Timestamp: {ist_time}")
                                except pyodbc.Error as e:
                                    print(f"Error executing SQL commands on {db_name}:", e)
                                finally:
                                    if cursor:
                                        cursor.close()
                except Exception as e:
                    print("Error processing an item in JSON data:", e)

        except json.JSONDecodeError as e:
            print("Invalid JSON payload received:", e)

    except Exception as e:
        print("Error in on_message callback:", e)

# MQTT setup
try:
    broker = "localhost"  # MQTT broker address
    port = 1883           # MQTT broker port
    topic = "u/6772556a2ab2030007d86b1b/+/r"  # Topic you subscribed to (can be dynamic)

    # Define connection strings
    connection_string1 = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=Kanhaiya\\SQLEXPRESS;"
        "Database=demo;"
        "Trusted_Connection=yes;"
    )
    connection_string2 = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=ManojPC\\SQLEXPRESS;"
        "Database=demo1;"
        "UID=test2;"
        "PWD=Exact123@;"
    )

    # Initialize connection managers
    cnx1 = ConnectionManager(connection_string1)
    cnx2 = ConnectionManager(connection_string2)

    # Store in a dictionary for easy access
    db_connections = {
        "Database1": cnx1,
        "Database2": cnx2
    }

    client = mqtt.Client()
    client.on_message = on_message

    client.connect(broker, port)
    print(f"Subscribed to topic: {topic}")

    client.subscribe(topic)

    # Start the MQTT loop to listen for messages
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nProgram interrupted. Exiting gracefully...")
        client.disconnect()  # Disconnect the MQTT client
        for db_manager in db_connections.values():
            db_manager.close_connection()
        print("MQTT client disconnected.")
    except Exception as e:
        print("Error in MQTT loop:", e)

except Exception as e:
    print("Error in MQTT setup or connection:", e)
