"""
Versión de testing de main.py con mocks
Para ejecutar tests sin servicios reales de Google Cloud
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
import time
from datetime import datetime
from unittest.mock import Mock, MagicMock

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Crear aplicación Flask
app = Flask(__name__)
CORS(app)

def get_mock_services():
    """Retorna servicios mock para testing"""
    # Mock de BigQuery service
    mock_bigquery_service = Mock()
    mock_bigquery_service.obtener_empresas_no_scrapeadas_batch.return_value = [
        {"biz_name": "Test Company 1", "biz_identifier": "TEST001"},
        {"biz_name": "Test Company 2", "biz_identifier": "TEST002"},
        {"biz_name": "Test Company 3", "biz_identifier": "TEST003"}
    ]
    
    # Mock de PubSub service
    mock_pubsub_service = Mock()
    mock_pubsub_service.publish_message.return_value = True
    
    return mock_bigquery_service, mock_pubsub_service

@app.route("/status", methods=['GET'])
def health_check():
    return {"status": "OK", "environment": "test"}

@app.route("/companies", methods=['GET'])
def get_companies_from_bigquery():
    """Endpoint mock para obtener empresas"""
    start_time = time.time()
    
    try:
        batch_size = request.args.get('batch_size', 1000)
        
        bigquery_service, _ = get_mock_services()
        companies = bigquery_service.obtener_empresas_no_scrapeadas_batch(batch_size, "test_table")
        
        logger.info(f"✅ Empresas obtenidas correctamente: {len(companies)}")
        
        results = [dict(company) for company in companies]
        return jsonify({
            "success": True,
            "data": results,
            "time_taken": time.time() - start_time,
            "timestamp": datetime.now().isoformat(),
            "environment": "test"
        }), 200

    except Exception as error_message:
        print(f"Error al ejecutar la consulta: {error_message}")
        return jsonify({
            "success": False,
            "error": f"Error interno del servidor: {error_message}",
            "timestamp": datetime.now().isoformat(),
            "environment": "test"
        }), 500

@app.route("/contacts", methods=['POST'])
def post_contacts_to_bigquery():
    """Endpoint mock para insertar contactos"""
    start_time = time.time()
    
    try:
        if not request.is_json:
            return jsonify({
                "success": False,
                "error": "Content-Type debe ser application/json",
                "timestamp": datetime.now().isoformat(),
                "environment": "test"
            }), 400

        logger.info(f"✅ Iniciando inserción de contactos (TEST)")

        _, pub_sub_services = get_mock_services()

        data = request.get_json()
        
        data = {
            "biz_name": data.get("biz_name"),
            "biz_identifier": data.get("biz_identifier"),
            "full_name": data.get("full_name"),
            "role": data.get("role"),
            "phone_number": data.get("phone_number"),
            "cat": data.get("cat"),
            "web_linkedin_url": data.get("web_linkedin_url"),
            "src_scraped_dt": int(datetime.now().timestamp() * 1000000),            
            "src_scraped_name": data.get("src_scraped_name"),
            "phone_flg": data.get("phone_exists"),
        }
        
        # Debug: verificar estructura de datos
        logger.info(f"✅ Tipo de datos recibidos: {type(data)}")
        logger.info(f"✅ Datos recibidos: {data}")

        # Simular publicación en Pub/Sub
        publish_result = pub_sub_services.publish_message(data)
        
        logger.info(f"✅ Mensaje publicado exitosamente en Pub/Sub (TEST)")
        return jsonify({
            "success": True,
            "message": "Datos enviados exitosamente a Pub/Sub (TEST)",
            "message_id": "test-message-id-123",
            "timestamp": datetime.now().isoformat(),
            "environment": "test"
        }), 200
        
    except Exception as error_message:
        print(f"Error al publicar mensaje en Pub/Sub: {error_message}")
        return jsonify({
            "success": False,
            "error": f"Error interno del servidor: {error_message}",
            "timestamp": datetime.now().isoformat(),
            "environment": "test"
        }), 500

if __name__ == "__main__":
    print("🚀 Iniciando servidor de testing en puerto 8080...")
    print("📝 Este es un servidor de testing con mocks - no usa servicios reales de Google Cloud")
    app.run(host="0.0.0.0", port=8080, debug=True)
