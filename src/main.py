from flask import Flask, request, jsonify
from flask_cors import CORS
from config import Config
import logging
from bigquery_services import BigQueryService
import time
from datetime import datetime



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Crear aplicación Flask
app = Flask(__name__)
CORS(app)  # Habilitar CORS para requests cross-origin

bigquery_service = None


def get_services():
    try: 
        # Inicializar BigQuery service
        bigquery_service = BigQueryService(
            project=Config.GOOGLE_CLOUD_PROJECT_ID,
            dataset=Config.BIGQUERY_DATASET
        )
        logger.info("✅ BigQuery Service inicializado correctamente")
    except Exception as e:
        logger.error(f"❌ Error inicializando servicios: {e}")
        raise

    return bigquery_service

def validate_request_data(request):
    if not request.is_json:
        return jsonify({
            "success": False,
            "error": "Content-Type debe ser application/json",
            "timestamp": datetime.now().isoformat()
        }), 400




@app.route("/status", methods=['GET'])
def health_check():
    return {"status": "OK"}

@app.route("/companies", methods=['GET'])
def get_companies_from_bigquery():
    """
        Obtener múltiples empresas en una sola consulta BigQuery
        Retorna: [
            {
                'biz_name': str,
                'biz_identifier': str
            }
        ]
        """
    start_time = time.time()
    
    try:
        """
        validate_request_data(request)
"""
        #data = request.get_json()
        #batch_size = data.get('batch_size', 1000)
        batch_size = request.args.get('batch_size', 1000)
        
        bigquery_service = get_services()
        # Obtener empresas no scrapeadas
        companies = bigquery_service.obtener_empresas_no_scrapeadas_batch(batch_size, Config.SOURCE_TABLE_NAME)
        
        logger.info(f"✅ Empresas no scrapeadas obtenidas correctamente: {len(companies)}")
        
        # Procesa los resultados y los convierte en una lista de diccionarios.
        results = [dict(company) for company in companies]
        return jsonify({
            "success": True,
            "data": results,
            "time_taken": time.time() - start_time,
            "timestamp": datetime.now().isoformat()
        }), 200

    except Exception as error_message:
        # Manejo de errores: Si algo falla, devuelve un error 500.
        # Es crucial para identificar problemas en un entorno de producción.
        print(f"Error al ejecutar la consulta de BigQuery: {error_message}")
        return jsonify({
            "success": False,
            "error": f"Error interno del servidor: {error_message}",
            "time_taken": time.time() - start_time,
            "timestamp": datetime.now().isoformat()
        }), 500


@app.route("/companies", methods=['PATCH'])
def patch_companies_in_bigquery():
    """
        Actualizar empresas en BigQuery
        """
    start_time = time.time()
    
    try:
        if not request.is_json:
            return jsonify({
                "success": False,
                "error": "Content-Type debe ser application/json",
                "timestamp": datetime.now().isoformat()
            }), 400
            
        logger.info(f"✅ Iniciando actualización de empresas en BigQuery")
        
        bigquery_service = get_services()
        
        data = request.get_json()

        biz_identifier = data.get('biz_identifier')
        biz_name = data.get('biz_name')
        contact_found_flg = data.get('contact_found_flg')
        
        # Debug: verificar estructura de datos
        logger.info(f"✅ Tipo de datos recibidos: {type(data)}")
        logger.info(f"✅ Datos recibidos: {data}")
        
        bigquery_service.actualizar_empresas_en_bigquery(Config.SOURCE_TABLE_NAME, biz_identifier, biz_name, contact_found_flg)

        return jsonify({
            "success": True,
            "message": f"Empresa actualizada correctamente: {biz_identifier}",
            "time_taken": time.time() - start_time,
            "timestamp": datetime.now().isoformat()
        }), 200

    except Exception as error_message:
        print(f"Error al actualizar empresas en BigQuery: {error_message}")
        return jsonify({
            "success": False,
            "error": f"Error interno del servidor: {error_message}",
            "time_taken": time.time() - start_time,
            "timestamp": datetime.now().isoformat()
        }), 500
    


@app.route("/contacts", methods=['POST'])
def post_contacts_to_bigquery():
    """
        Insertar datos en BigQuery
        """
    start_time = time.time()
    
    try:
        if not request.is_json:
            return jsonify({
                "success": False,
                "error": "Content-Type debe ser application/json",
                "timestamp": datetime.now().isoformat()
            }), 400

        logger.info(f"✅ Iniciando inserción de contactos en BigQuery")

        bigquery_service = get_services()

        data = request.get_json()
        
        # Debug: verificar estructura de datos
        logger.info(f"✅ Tipo de datos recibidos: {type(data)}")
        logger.info(f"✅ Datos recibidos: {data}")
        bigquery_service.insertar_contactos_en_bigquery(Config.DESTINATION_TABLE_NAME, data)

        logger.info(f"✅ Contactos insertados correctamente: {len(data)}")

        return jsonify({
            "success": True,
            "timestamp": datetime.now().isoformat()
        }), 200
    
    except Exception as error_message:
        print(f"Error al insertar datos en BigQuery: {error_message}")
        return jsonify({
            "success": False,
            "error": f"Error interno del servidor: {error_message}",
            "timestamp": datetime.now().isoformat()
        }), 500





if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)