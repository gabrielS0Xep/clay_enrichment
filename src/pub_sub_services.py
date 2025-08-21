import os
from google.cloud import pubsub_v1

class PubSubService:
    def __init__(self, project_id:str, topic_name:str):
        self.project_id = project_id
        self.topic_name = topic_name
        self.publisher = pubsub_v1.PublisherClient()


    def publish_message(self, data : dict):
        """Publish message to pubsub"""
        data = data.encode("utf-8")
        future = self.publisher.publish(self.topic_name, data)
        
        try:
            future.result()
            return True
        except Exception as error_message:
            raise error_message
    