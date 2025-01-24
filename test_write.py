import paho.mqtt.client as mqtt
import random
import time
import datetime

# Callback function when a message has been published successfully
def on_publish(client, userdata, mid):
    print(f"Message published successfully: {mid}")

# Callback function when an error occurs during message publishing
def on_publish_fail(client, userdata, mid):
    print(f"Message failed to publish: {mid}")

def generate_dynamic_tag():
    """Generate a dynamic tag in the format 'teXXXX'."""
    return f"test{random.randint(1000, 9999)}"

def generate_random_value():
    """Generate a random value for 'r' between 0 and 1, rounded to two decimal places."""
    return round(random.random(), 2)  # Generates a random float between 0.0 and 1.0


def publish_mqtt_message(client):
    """Publish an MQTT message with a dynamic tag and timestamp."""
    # Get the current timestamp in milliseconds
    timestamp = int(datetime.datetime.now().timestamp() * 1000)
    
    # Generate dynamic tag and random value for "r"
    dynamic_tag = generate_dynamic_tag()
    r_value = generate_random_value()

    # Prepare the message payload
    payload = f'[{{"t": {timestamp}, "r": {r_value}}}]'

    # Prepare the topic
    topic = f"u/6772556a2ab2030007d86b1b/{dynamic_tag}/r"
    
    try:
        # Publish the message
        result = client.publish(topic, payload=payload)
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"Message published successfully to topic: {topic}")
        else:
            print(f"Failed to publish message to topic: {topic}")

    except Exception as e:
        print(f"Error publishing message: {e}")

def main():
    """Run the script to publish messages every 10 seconds."""
    # Create a new MQTT client instance
    client = mqtt.Client()

    # Set the callbacks for publishing success or failure
    client.on_publish = on_publish

    # Connect to the local MQTT broker
    client.connect("localhost", 1883, 60)
    while True:
        publish_mqtt_message(client)
        time.sleep(10)
    
    # Loop forever
    client.loop_forever()

if __name__ == "__main__":
    main()
