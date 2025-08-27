"""
Configuración de waterfall enrichment
Maneja variables de entorno y configuraciones del proyecto
"""

import os
from dotenv import load_dotenv
import json
from secret_manager import SecretManager
# Cargar variables de entorno desde .env
load_dotenv()

class Config:
    # Configuración API Key
    API_KEY = os.getenv('API_KEY', 'MxJq2{!8z^3YJ8oj]cP2GI')

    # Configuración Google Cloud Project ID
    GOOGLE_CLOUD_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT_ID','qa-cdp-mx')
    # Configuración BigQuery
    BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'raw_in_scrapper')
    SOURCE_TABLE_NAME = os.getenv('GOOGLE_BIGQUERY_TABLE','clay_scraped_companies')
    DESTINATION_TABLE_NAME = os.getenv("GOOGLE_BIGQUERY_TABLE_DESTINATION","clay_scraped_contacts")
    # Configuración Pub/Sub
    PUBSUB_TOPIC_CONTACTS = os.getenv('PUBSUB_TOPIC_CONTACTS', 'enriched_contacts')
    PUBSUB_TOPIC_COMPANIES = os.getenv('PUBSUB_TOPIC_COMPANIES', 'scraped_companies')
    # Configuración Cloud Tasks
    CLOUD_TASKS_QUEUE = os.getenv('CLOUD_TASKS_QUEUE', 'waterfall-enrichment-queue')
    CLOUD_TASKS_LOCATION = os.getenv('CLOUD_TASKS_LOCATION', 'us-central1')
    CLOUD_TASKS_URL = os.getenv('CLOUD_TASKS_URL', 'https://api.clay.com/v3/sources/webhook/pull-in-data-from-a-webhook-6b71c86f-e6b9-47bb-a355-9d38c07488fe')

    """Clase de configuración para el Waterfall Enrichment"""
   
    # scrapper_secret = secretManager.get_secret('linkedin_scrapper')


    # Service Account Configuration - múltiples opciones
    # GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')  
    
    # Configuración Flask
    FLASK_HOST = os.getenv('FLASK_HOST', '0.0.0.0')
    PORT = int(os.getenv('PORT', '5000'))
    FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    
    # Timeout para requests
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '300'))  # 5 minutos
    
    # Configuración de reintentos y timeouts
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))  # Número máximo de reintentos
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))  # Segundos entre reintentos
    MAX_BATCH_SIZE = int(os.getenv('MAX_BATCH_SIZE', '1000'))
    BATCH_TIMEOUT = int(os.getenv('BATCH_TIMEOUT', '600'))  # 10 minutos para batch completo
    INDIVIDUAL_TIMEOUT = int(os.getenv('INDIVIDUAL_TIMEOUT', '120'))  # 2 minutos por empresa

    @classmethod
    def validate(cls):
        """Valida que todas las variables de entorno requeridas estén configuradas"""
        # Variables siempre requeridas
        required_vars = [
            #('SERPER_API_KEY', cls.SERPER_API_KEY),
            # ('GOOGLE_CLOUD_PROJECT_ID', cls.GOOGLE_CLOUD_PROJECT_ID),
            #('ENRICHLAYER_API_KEY', cls.ENRICHLAYER_API_KEY)
        ]
        
        missing_vars = []
        for var_name, var_value in required_vars:
            if not var_value:
                missing_vars.append(var_name)
        
        # # Validar que al menos una forma de autenticación esté configurada
        # auth_methods = [
        #     cls.GOOGLE_APPLICATION_CREDENTIALS,
        # ]
        
        # if not any(auth_methods):
        #     missing_vars.append("GOOGLE_APPLICATION_CREDENTIALS o ENRICHLAYER_API_KEY")
        
        if missing_vars:
            raise ValueError(f"Faltan las siguientes variables de entorno: {', '.join(missing_vars)}")
        
        return True 