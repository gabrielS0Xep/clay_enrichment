from flask import Flask, request, jsonify
from flask_cors import CORS
from config import Config
import logging
from bigquery_services import BigQueryService
from pub_sub_services import PubSubService
from firebase_services import FirestoreService
import time 
from datetime import datetime, date
from functools import wraps
from cloud_tasks import CloudTasks
import json


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
        cloud_tasks_service = CloudTasks(
            project=Config.GOOGLE_CLOUD_PROJECT_ID,
            location=Config.CLOUD_TASKS_LOCATION,
            queue=Config.CLOUD_TASKS_QUEUE
        )

        logger.info("✅ Servicios inicializados correctamente")
    except Exception as e:
        logger.error(f"❌ Error inicializando servicios: {e}")
        raise

    return bigquery_service , pub_sub_services, cloud_tasks_service

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
        
        bigquery_service, _, _ = get_services()
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
        
        _, pub_sub_services, _ = get_services()
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
    
    try:
        if not request.is_json:
            return jsonify({
                "success": False,
                "error": "Content-Type debe ser application/json",
                "timestamp": datetime.now().isoformat()
            }), 400

        logger.info(f"✅ Iniciando inserción de contactos en BigQuery")

        _ , pub_sub_services, _ = get_services()

        data = request.get_json()
        
        data = {
            "biz_name": data.get("biz_name",""),
            "biz_identifier": data.get("biz_identifier",""),
            "full_name": data.get("full_name",""),
            "role": data.get("role",""),
            "phone_number": data.get("phone_number",""),
            "cat": data.get("cat",""),
            "web_linkedin_url": data.get("web_linkedin_url",""),
            "src_scraped_dt": int(datetime.now().timestamp() * 1000000),            
            "src_scraped_name": data.get("src_scraped_name",""),
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

    try:
        logger.info(f"✅ Datos recibidos: {data}")
        logger.info(f"✅ URL: {url}")
        logger.info(f"tipo de datos: {type(data)}")
        logger.info(f"tipo de url: {type(url)}")
        json_payload = data

        cloud_tasks_service.create_http_task(
           url=url,
           json_payload=json_payload,
           headers={"Content-type": "application/json"}
        )
        return jsonify({
            "success": True,
            "message": "Tarea creada correctamente",
            "timestamp": datetime.now().isoformat()
        }), 200


    except ValueError as error_message:
        logger.error(f"❌ Error de validación: {error_message}")
        return jsonify({
            "success": False,
            "error": str(error_message),
            "timestamp": datetime.now().isoformat()
        }), 400
    except Exception as error_message:
        logger.error(f"❌ Error al crear la tarea: {error_message}")
        return jsonify({
            "success": False,
            "error": f"Error interno del servidor: {error_message}",
            "timestamp": datetime.now().isoformat()
        }), 500


@app.route("/contacts/enrichment", methods=['POST'])
def post_contacts_enrichment():
    """
        Enrichment de contactos mediante la publicacion de los mismos en cloud tasks
        para evitar bloqueos de bigquery

        Se debe enviar el listado de biz_identifiers de las empresas que se quieren enrichar
        Body JSON(Requerido):
        {
            contacts: [
                {
                    "web_linkedin_url": "https://www.linkedin.com/in/contact1",
                    "biz_identifier": "biz_identifier1",
                    "biz_name": "Nombre de la empresa",
                    "role": "Rol del contacto",
                    "full_name": "Nombre completo del contacto",
                    "cat": "Categoria"
                },
                .
                .
                .
                {
                    "web_linkedin_url": "https://www.linkedin.com/in/contact2",
                    "biz_identifier": "biz_identifier2",
                    "biz_name": "Nombre de la empresa",
                    "role": "Rol del contacto",
                    "full_name": "Nombre completo del contacto",
                    "cat": "Categoria"
                }
            ]
        }

        Retorna:
        {
            "Empresas ya scrapeadas anteriormente": ["biz_identifier1", "biz_identifier2", "biz_identifier3"],
            "Empresas no scrapeadas": ["biz_identifier4", "biz_identifier5", "biz_identifier6"],
            "msg: "Enriquecimiento creado correctamente para las empresas no scrapeadas"
            "timestamp": datetime.now().isoformat()
        }
        Si hay un error, retorna:
        {
            "success": False,
            "error": "Error interno del servidor",
            "timestamp": datetime.now().isoformat()
        }
    """
    try:
        bigquery_service, _, cloud_tasks_service = get_services()
        url = Config.CLAY_WEBHOOK_URL

        headers = {
            "Content-type": "application/json",
            f'{Config.CLAY_WEBHOOK_HEADER}': f'{Config.CLAY_WEBHOOK_KEY}'
        }
        

        data = request.get_json()
        logger.info(f"✅ Datos recibidos: {data}")
        if not data.get("contacts"):
            return jsonify({
                "success": False,
                "error": "Contacts is required",
                "timestamp": datetime.now().isoformat()
            }), 400

        contacts = data.get("contacts")
        logger.info(f"✅ Contacts: {contacts}")
        contacts_urls = [
            contact.get("web_linkedin_url")
            for contact in contacts
            if contact.get("web_linkedin_url")
        ]
        logger.info(f"✅ Contacts URLs: {contacts_urls}")

        contacts_already_scraped_df = bigquery_service.verify_if_contacts_was_scraped(Config.DESTINATION_TABLE_NAME, contacts_urls)
        logger.info(f"✅ Contacts already scraped: {contacts_already_scraped_df}")
        
        # Extraer las URLs de los contactos ya scrapeados
        if contacts_already_scraped_df is not None and not contacts_already_scraped_df.empty:
            scraped_urls = set(contacts_already_scraped_df['web_linkedin_url'].tolist())
        else:
            scraped_urls = set()
        
        logger.info(f"✅ Scraped URLs: {scraped_urls}")
        
        # Filtrar los contactos que NO fueron scrapeados
        contacts_not_scraped = [
            contact for contact in contacts
            if contact.get("web_linkedin_url") not in scraped_urls
        ]
        
        logger.info(f"✅ Contacts not scraped: {len(contacts_not_scraped)} de {len(contacts)}")
        
        if not contacts_not_scraped:
            return jsonify({
                "success": True,
                "message": "Todas las contactos ya fueron scrapeadas",
                "timestamp": datetime.now().isoformat()
            }), 200
    
        logger.info(f"✅ URL: {url}")
        logger.info(f"tipo de datos: {type(contacts_not_scraped)}")

        # El base_payload debe mantener los otros campos del request original (si los hay)
        base_payload = {k: v for k, v in data.items() if k != "contacts"}
        max_payload_bytes = 90 * 1024  # 900KB
        logger.info(f"✅ Max payload bytes: {max_payload_bytes}")

        def payload_size(contacts_chunk: list[dict]) -> int:
            payload = {**base_payload, "contacts": contacts_chunk}
            return len(json.dumps(payload).encode("utf-8"))

        chunks = []
        current_chunk = []

        for contact in contacts_not_scraped:
            tentative_chunk = current_chunk + [contact]
            if payload_size(tentative_chunk) <= max_payload_bytes:
                current_chunk = tentative_chunk
                continue
            logger.info(f"✅ Current chunk: {current_chunk}")
            if not current_chunk:
                raise ValueError("CONTACT_PAYLOAD_EXCEEDS_100KB_LIMIT")

            chunks.append(current_chunk)
            current_chunk = [contact]

        if current_chunk:
            chunks.append(current_chunk)

        logger.info(f"✅ Enviando {len(chunks)} payload(s) a Cloud Tasks")
        logger.info(f"✅ Chunks: {chunks}")
        logger.info(f"✅ Base payload: {base_payload}")
        logger.info(f"✅ URL: {url}")

        # Validar y actualizar contadores en Firebase antes de enviar
        try:

            firebase_service = FirestoreService(project=Config.FIREBASE_PROJECT_ID, database=Config.FIREBASE_DATABASE)
            limit = int(Config.CLAY_LIMITS)
            documents_names = [Config.FIREBASE_DOCUMENT_TABLES, Config.FIREBASE_DOCUMENT_REQUEST_APOLLO, Config.FIREBASE_DOCUMENT_REQUEST_IMPORT]
            for document_name in documents_names:
                logger.info(f"Collection: {Config.FIREBASE_COLLECTION}")
                logger.info(f"✅ Validando límites en Firebase para el documento: {document_name}")
                logger.info(f"✅ New count: {len(contacts_not_scraped) if document_name == Config.FIREBASE_DOCUMENT_TABLES else len(chunks)}")
                logger.info(f"✅ Advertising threshold: {int(Config.CLAY_LIMIT_ADVERTISING) if Config.CLAY_LIMIT_ADVERTISING else 40000}")
                logger.info(f"✅ Limit: {limit}")
                logger.info(f"✅ Document name: {document_name}")
                count_to_increment = len(contacts_not_scraped) if document_name == Config.FIREBASE_DOCUMENT_TABLES else len(chunks)
                logger.info(f"✅ Count to increment: {count_to_increment}")
                limit_exceeded, advertising_threshold_exceeded = firebase_service.validate_limit_and_advertising_threshold(
                    collection=Config.FIREBASE_COLLECTION,
                    document_name=document_name,
                    new_count= count_to_increment,
                    advertising_threshold=int(Config.CLAY_LIMIT_ADVERTISING),
                    limit=limit
                )
                if limit_exceeded:
                    return jsonify({
                        "success": False,
                        "error": f"Límite excedido en el documento: {document_name}",
                        "timestamp": datetime.now().isoformat()
                    }), 429
                
                
            for document_name in documents_names:
                firebase_service.increment_current_count(
                    collection=Config.FIREBASE_COLLECTION,
                    document_name=document_name,
                    increment=len(chunks)
                )
            
            logger.info(f"✅ Firebase contadores actualizados: {documents_names}")
        except Exception as firebase_error:
            error_message = str(firebase_error)
            logger.error(f"❌ Error validando límites en Firebase: {error_message}")
            return jsonify({
                "success": False,
                "error": f"Error interno del servidor: {error_message}",
                "timestamp": datetime.now().isoformat()
            }), 500

        for chunk in chunks:
            json_payload = {**base_payload, "contacts": chunk}
            cloud_tasks_service.create_http_task(
                url=url,
                json_payload=json_payload,
                headers=headers
            )

        return jsonify({
            "success": True,
            "message": "Tarea creada correctamente",
            "timestamp": datetime.now().isoformat()
        }), 200

    except Exception as error_message:
        logger.error(f"❌ Error al crear la tarea: {error_message}")
        return jsonify({
            "success": False,
            "error": f"Error interno del servidor: {error_message}",
            "timestamp": datetime.now().isoformat()
        }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)