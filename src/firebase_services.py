from google.cloud import firestore
from google.cloud.firestore_v1.base_client import BaseClient # Para tipado
from google.cloud.firestore_v1.client import Client
from logging import Logger
import logging
# Inicialización del cliente de Firestore.
logger: Logger = logging.getLogger(__name__)
class FirestoreService:

    def __init__(self, project:str, database:str):
        try:
            self.db: Client = firestore.Client(project=project,database=database)
            logger.info(f"✅ Cliente de Firestore inicializado: proyecto={project}")
        except Exception as e:
            logger.error(
                f"❌ Error inicializando cliente de Firestore: {e}\n"
                f"   Proyecto: {project}\n"
            )
            raise

    def get_current_count(self,collection:str, document_name:str) -> int:
        return self.db.collection(collection).document(document_name).get().to_dict()['count']

    def update_current_count(self,collection:str, document_name:str, count:int) -> None:
        self.db.collection(collection).document(document_name).set({'count': count})
    
    def increment_current_count(self,collection:str, document_name:str, increment:int) -> None:
        logger.info(f"Incrementando contador de {document_name} en {collection} en {increment} unidades")
        self.db.collection(collection).document(document_name).update({'count': firestore.Increment(increment)})

    def calculate_new_count(self,collection:str, document_name:str, count:int) -> int:
        return self.get_current_count(collection, document_name) + count
    
    def validate_limit_and_advertising_threshold(
        self,
        collection: str,
        document_name: str,
        new_count: int,
        advertising_threshold: int = 40000,
        limit: int = 50000
    ) -> dict:   
        """
        Valida límites y thresholds de document_name en Firebase (sin actualizar).
        
        Args:
            collection: Nombre de la colección en Firebase
            document_name: Nombre del documento en Firebase
            new_count: Nuevo valor a calcular
            advertising_threshold: Umbral de advertencia para logging (opcional, por defecto 40000)
            limit: Límite máximo de document_name (por defecto 50000)
        Returns:
            dupla de bools con información del estado de la validación
        Raises:
            Exception: Si se superan los límites establecidos
        """
        try:
            current_count = self.get_current_count(collection, document_name)
            # Calcular nuevos valores
            new_count_result = current_count + new_count
            limit_exceeded = new_count_result > limit
            # Validar límite de chunks
            if limit_exceeded is not None:
                if limit_exceeded:
                    logger.error(
                        f"LÍMITE EXCEDIDO: El límite de {document_name} ({limit}) sería superado. "
                        f"Actual: {current_count}, Intento agregar: {new_count}, "
                        f"Nuevo total: {new_count_result}"
                        f"se debe realizar una limpieza de tabla o webhook y enviar la notificacion a slack"
                    )
                # Verificar umbral de advertising y loguear si se supera
            advertising_threshold_exceeded = new_count_result > advertising_threshold
            if advertising_threshold_exceeded is not None:
                if advertising_threshold_exceeded:
                    logger.warning(
                        f"⚠️ UMBRAL DE ADVERTISING SUPERADO: "
                        f"El contador de {document_name} ({new_count_result}) ha superado el umbral de advertising "
                        f"({advertising_threshold}). Límite total: {limit}"
                    )
            
            return limit_exceeded, advertising_threshold_exceeded
            
        except Exception as error:
            logger.error(f"❌ Error validando límites en Firebase: {error}")
            raise
    
    