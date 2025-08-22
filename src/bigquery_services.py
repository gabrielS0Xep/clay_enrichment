from datetime import datetime, date
from math import log
import os
import pandas as pd
from typing import List, Dict
import csv
from logging import Logger
import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from pandas_gbq import to_gbq

logger: Logger = logging.getLogger(__name__)

class BigQueryService:

    def __init__(self, project:str, dataset:str) -> None:
        self.__project_id = project
        self.__dataset = dataset
        self.__bq_client = bigquery.Client(project=self.__project_id) 
    def create_table_clay_scraped_companies(self, table_name:str):
        """Crea la tabla de control empresas_scrapeadas_linkedin si no existe - CON RECREACIÓN FORZADA"""

        dataset_id = self.__dataset
        table_id = table_name

        # Schema de la tabla de control
        schema = [
            bigquery.SchemaField("biz_identifier", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("biz_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("scrapping_d", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("contact_found_flg", "BOOLEAN", mode="NULLABLE")
        ]

        # Crear referencia a la tabla
        dataset_ref = self.__bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        try:
            # Verificar si la tabla ya existe
            self.__bq_client.get_table(table_ref)
            logger.info(f"ℹ️ La tabla {dataset_id}.{table_id} ya existe")
        except NotFound:
            # La tabla no existe, crearla
            try:
                table = bigquery.Table(table_ref, schema=schema)
                table = self.__bq_client.create_table(table)
                logger.info(f"✅ Tabla de control {dataset_id}.{table_id} creada exitosamente con schema correcto")
                return table
            except Exception as e:
                logger.error(f"❌ Error creando tabla: {e}")
                raise
        except Exception as e:
                logger.error(f"❌ Error verificando tabla: {e}")
                raise

    def create_table_linkedin_info(self, table_name:str):
        """Crea la tabla linkedin_info si no existe"""

        dataset_id = self.__dataset
        table_id = table_name

        # Schema completo de la tabla linkedin_info
        schema = [
            bigquery.SchemaField("biz_identifier", "STRING", mode="R"),
            bigquery.SchemaField("biz_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("full_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("role", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("phone_number", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("cat", "STRING", mode="NULLABLE"), 
            bigquery.SchemaField("web_linkedin_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("src_scraped_dt", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("src_scraped_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("phone_flg", "BOOLEAN", mode="NULLABLE")
        ]

        # Crear referencia a la tabla
        dataset_ref = self.__bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        try:
            # Intentar obtener la tabla (si existe)
            self.__bq_client.get_table(table_ref)
            logger.info(f"✅ Tabla de datos {dataset_id}.{table_id} ya existe")
        except NotFound:
            # Si no existe, crearla
            table = bigquery.Table(table_ref, schema=schema)
            table = self.__bq_client.create_table(table)
            logger.info(f"✅ Tabla de datos {dataset_id}.{table_id} creada exitosamente")

    def obtener_empresas_no_scrapeadas_batch(self, batch_size: int , table_name: str) -> Dict[str, Dict]:
        """
        Obtener múltiples empresas en una sola consulta BigQuery
        Retorna: [
            {
                'biz_name': str,
                'biz_identifier': str
            }
        ]
        """
        project_id = self.__project_id
        dataset_id = self.__dataset
        table_id = table_name
        
        try:
            
            # Query para obtener todas las empresas de una vez
            where_clause = "(contact_found_flg = 0 or contact_found_flg is null) and scrapping_d is null"
            query = f"SELECT biz_identifier, biz_name FROM `{project_id}.{dataset_id}.{table_id}` WHERE {where_clause} LIMIT {batch_size}"

            query_job = self.__bq_client.query(query)
            logger.info(f"✅ Consulta BigQuery ejecutada correctamente ")
            results = list(query_job.result())
        
            return results

        except Exception as e:
            logger.error(f"❌ Error obteniendo empresas no scrapeadas en batch: {e}")
            # En caso de error, asumir que todas necesitan scraping
            result = {}
            return result


    def actualizar_empresas_scrapeadas(self, table_name:str, biz_identifier:str, biz_name:str, contact_found_flg:bool):
        """Actualiza los datos de scraping de una empresa en la tabla de control"""
        dataset_id = self.__dataset
        table_id = table_name
        project_id = self.__project_id
        
        try:
            # Query para actualizar
            query = f"""
            UPDATE `{project_id}.{dataset_id}.{table_id}`
            SET scrapping_d = @scraping_d, contact_found_flg = @contact_found_flg
            WHERE biz_identifier = @biz_identifier AND biz_name = @biz_name
            """
            logger.info(f"Dato antes de pasar if: {contact_found_flg}")
            # Forzamos tipo bool seguro para evitar errores de tipo
            contact_found_flg = contact_found_flg if isinstance(contact_found_flg, bool) else bool(contact_found_flg)
            
            logger.info(f"Dato post Pasar if: {contact_found_flg}")

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("biz_identifier", "STRING", biz_identifier),
                    bigquery.ScalarQueryParameter("biz_name", "STRING", biz_name),
                    bigquery.ScalarQueryParameter("scraping_d", "DATE", date.today()),
                    bigquery.ScalarQueryParameter("contact_found_flg", "INT64", int(contact_found_flg,False)),
                ]
            )
            
            query_job = self.__bq_client.query(query, job_config=job_config)
            query_job.result()  # Esperar a que termine
            
            logger.info(f"✅ Empresa {biz_name} actualizada en tabla de control")
            
        except Exception as e:
            logger.error(f"❌ Error actualizando empresa scrapeada: {e}")
            raise
            
    def push_to_pubsub(self, data:Dict):
        """Push data to pubsub"""
        
    