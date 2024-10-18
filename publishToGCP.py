import json
import os
import sys
import configparser
# import random
# import string
import uuid

from google.cloud import pubsub_v1
from datetime import datetime

def loadConfigurations(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

def loadPayload(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)
    
def generateRandomContentId():
    # random_number = ''.join(random.choices(string.digits, k=8))
    # random_string = ''.join(random.choices(string.ascii_uppercase, k=3))
    # return f"{random_number}-{random_string}-GR-RU-NL"
    uniqueId = str(uuid.uuid4())
    return f"{uniqueId}"

def publishMessagestoGCP(config):
    # Create a Pub/Sub client
    publisher = pubsub_v1.PublisherClient.from_service_account_json(config['PubSub']['service_account_key'])
    
    topic_path = config['PubSub']['topic']
    send_limit = int(config['Payload']['sendLimit'])
    payload_file = config['Payload']['payload_file']

    # Load the JSON payload
    message_template = loadPayload(payload_file)

    for i in range(send_limit):
        
        message = message_template.copy() 
        message['contents'][0]['metaDataHeader']['contentLatestUpdateDate'] = datetime.utcnow().isoformat() + 'Z'
        message['contents'][0]['metaDataHeader']['contentId'] = generateRandomContentId()
        message_data = json.dumps(message).encode('utf-8')
        print(message)
        future = publisher.publish(topic_path, message_data)
        print(f"Published message ID: {future.result()}")

if __name__ == "__main__":
    
    config_file = "config.ini"
    if not os.path.exists(config_file):
        print(f"Configuration file {config_file} does not exist.")
        sys.exit(1)

    config = loadConfigurations(config_file)
    publishMessagestoGCP(config)
