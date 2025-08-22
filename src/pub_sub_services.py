import os
from google.cloud import pubsub_v1
import json

class PubSubService:
    def __init__(self, project_id:str, topic_name:str):
        self.project_id = project_id
        self.topic_name = topic_name
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_name)

    def publish_message(self, data : dict):
        """Publish message to pubsub"""

       
        future = self.publisher.publish(self.topic_path, json.dumps(data).encode("utf-8"))
        
        try:
            future.result()
            return True
        except Exception as error_message:
            raise error_message
    