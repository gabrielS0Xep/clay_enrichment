import os
from google.cloud import pubsub_v1
import json

class PubSubService:
    def __init__(self, project_id:str):
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()

    def publish_message(self, topic_name:str, data : dict):
        """Publish message to pubsub"""
        topic_path = self.publisher.topic_path(self.project_id, topic_name)

        try:
            future = self.publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
    
            return "OK"
        except Exception as error_message:
            raise error_message
    