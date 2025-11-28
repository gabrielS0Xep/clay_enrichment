from slack import WebClient
from slack.errors import SlackApiError
from config import Config
import logging
from typing import Dict

logger = logging.getLogger(__name__)

class SlackService:
    def __init__(self, bot_token: str, channel: str):
        self.client = WebClient(token=bot_token)
        self.channel = channel

    def send_message(self, message: Dict):
        try:
            self.client.chat_postMessage(channel=self.channel, text=message['text'])
        except SlackApiError as e:
            logger.error(f"Error sending message to Slack: {e}")
            return False
        return True

    def format_message(self, message: Dict):
        return {
            'text': message['text']
        }