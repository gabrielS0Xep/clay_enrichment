import os

class TestConfig:
    # Configuración para testing local con emuladores
    GOOGLE_CLOUD_PROJECT_ID = "test-project"
    BIGQUERY_DATASET = "test_dataset"
    PUBSUB_TOPIC_CONTACTS = "test-contacts-topic"
    SOURCE_TABLE_NAME = "test_source_table"
    DESTINATION_TABLE_NAME = "test_destination_table"
    
    # Variables de entorno para emuladores
    PUBSUB_EMULATOR_HOST = "localhost:8085"
    BIGQUERY_EMULATOR_HOST = "localhost:9050"
    
    @classmethod
    def setup_emulator_env(cls):
        """Configurar variables de entorno para emuladores"""
        os.environ["PUBSUB_EMULATOR_HOST"] = cls.PUBSUB_EMULATOR_HOST
        os.environ["BIGQUERY_EMULATOR_HOST"] = cls.BIGQUERY_EMULATOR_HOST
        os.environ["GOOGLE_CLOUD_PROJECT"] = cls.GOOGLE_CLOUD_PROJECT_ID
