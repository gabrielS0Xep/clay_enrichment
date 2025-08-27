from flask import Flask, request, jsonify
from flask_cors import CORS
from config import Config
import logging
from bigquery_services import BigQueryService
from pub_sub_services import PubSubService
import time 
from datetime import datetime, date
from functools import wraps
from cloud_tasks import CloudTasks

def require_api_key(func):
    @wraps(func)
    def decorated_function(*args, **kwargs):
        # La API Key se espera en el encabezado 'X-API-Key'
        key_from_request = request.headers.get('X-API-Key')
        
        # Compara la clave de la solicitud con la clave almacenada
        if key_from_request and key_from_request == Config.API_KEY:
            return func(*args, **kwargs)
        else:
            return jsonify({"error": "Unauthorized"}), 401
    return decorated_function



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Crear aplicación Flask
app = Flask(__name__)
CORS(app)  # Habilitar CORS para requests cross-origin

bigquery_service = None
pub_sub_services = None
cloud_tasks_service = None


def get_services():
    try: 
        # Inicializar BigQuery service
        bigquery_service = BigQueryService(
            project=Config.GOOGLE_CLOUD_PROJECT_ID,
            dataset=Config.BIGQUERY_DATASET
        )
        pub_sub_services = PubSubService(
            project_id= Config.GOOGLE_CLOUD_PROJECT_ID
            )
        logger.info("✅ Servicios inicializados correctamente")
    except Exception as e:
        logger.error(f"❌ Error inicializando servicios: {e}")
        raise

    return bigquery_service , pub_sub_services

def get_cloud_tasks_service():
    try:
        cloud_tasks_service = CloudTasks(
            project=Config.GOOGLE_CLOUD_PROJECT_ID,
            location=Config.CLOUD_TASKS_LOCATION,
            queue=Config.CLOUD_TASKS_QUEUE
        )
        return cloud_tasks_service
    except Exception as e:
        logger.error(f"❌ Error inicializando Cloud Tasks: {e}")
        raise

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

@require_api_key
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
        
        bigquery_service, _ = get_services()
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


@app.route("/companies/<string:biz_identifier>", methods=['PATCH'])
@require_api_key
def patch_companies_in_bigquery(biz_identifier):
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
        
        _, pub_sub_services = get_services()
        topic_name = Config.PUBSUB_TOPIC_COMPANIES

        data = request.get_json()
        data = {
            "biz_name": data.get("biz_name"),
            "biz_identifier": data.get("biz_identifier"),
            "contact_found_flg": data.get("contact_found_flg"),
            "scrapping_d": f"{date.today().strftime('%Y-%m-%d')}",
            "_CHANGE_TYPE": "UPSERT"
        }
        logger.info(f"✅ Datos a publicar en Pub/Sub: {data}")
        logger.info(f"✅ Topic name: {topic_name}")

        pub_sub_services.publish_message(topic_name, data)

       # bigquery_service.actualizar_empresas_scrapeadas(Config.SOURCE_TABLE_NAME, biz_identifier, biz_name, contact_found_flg)

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
        Insertar los datos en bigquery mediante la publicacion de los mismos en pubsub
        para evitar bloqueos de bigquery

        Body JSON(Requerido):
        {
    "biz_name": "Nombre de la empresa",
    "biz_identifier": "Identificador de la empresa",
    "full_name": "Nombre Completo ",
    "role": "Rol del contacto",
    "phone_number": "Numero de telefono parseado sin espacios ni guiones ni caracteres especiales",
    "cat": "Categoria",
    "web_linkedin_url": "URL de linkedin",
    "phone_exists": True,
    "src_scraped_name": "Nombre de la fuente donde se extrajo el contacto"
  }

    Retorna:
    {
        "success": True,
        "timestamp": datetime.now().isoformat()
    }

    Si hay un error, retorna:
    {
        "success": False,
        "error": "Error interno del servidor",
        "timestamp": datetime.now().isoformat()
    }

    Si el body no es JSON, retorna:

    {
        "success": False,
        "error": "Content-Type debe ser application/json",
        "timestamp": datetime.now().isoformat()
    }
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

        _ , pub_sub_services = get_services()

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
            "phone_flg": int(data.get("phone_exists", False)),
        }
        topic_name = Config.PUBSUB_TOPIC_CONTACTS

        # Debug: verificar estructura de datos
        logger.info(f"✅ Tipo de datos recibidos: {type(data)}")
        logger.info(f"✅ Datos recibidos: {data}")
        # Publicar mensaje en Pub/Sub
        publish_result = pub_sub_services.publish_message(topic_name, data)
        
        logger.info(f"✅ Mensaje publicado exitosamente en Pub/Sub.")
        return jsonify({
            "success": "True",
            "message": "Datos enviados exitosamente a Pub/Sub",
            "message_id": f"{publish_result}",
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as error_message:
        print(f"Error al publicar mensaje en Pub/Sub.")
        return jsonify({
            "success": "False",
            "error": f"Error interno del servidor: {error_message}",
            "timestamp": datetime.now().isoformat()
        }), 500


@app.route("/apollo_enrichment", methods=['POST'])
def post_waterfall_enrichment():
    """
        Enrichment de datos mediante la publicacion de los mismos en pubsub
        para evitar bloqueos de bigquery
    """
    start_time = time.time()
    cloud_tasks_service = get_cloud_tasks_service()

    data = request.get_json()
    url = Config.CLOUD_TASKS_URL

    logger.info(f"✅ Datos recibidos: {data}")
    logger.info(f"✅ URL: {url}")
    logger.info(f"tipo de datos: {type(data)}")
    logger.info(f"tipo de url: {type(url)}")
    json_payload = {
        "biz_name": data.get("biz_name"),
        "biz_identifier": data.get("biz_identifier"),
        "full_name": data.get("full_name"),
        "role": data.get("role"),
    }

    #cloud_tasks_service.create_http_task(
     #   url=url,
     #   json_payload=json_payload
    #)






if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)