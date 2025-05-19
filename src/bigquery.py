"""
Módulo para gestionar la interacción con Google BigQuery.

Este módulo proporciona funcionalidades para conectarse a BigQuery,
crear tablas, cargar datos, verificar la existencia de tablas y
otras operaciones relacionadas con la gestión de datos en BigQuery.
"""

import logging
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from src.table import Table 

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class BigQuery:
    """
    Clase para gestionar conexiones y operaciones con Google BigQuery.
    
    Esta clase proporciona métodos para establecer conexión con BigQuery,
    crear tablas, cargar datos, verificar la existencia de recursos y
    otras operaciones relacionadas con la gestión de datos.
    """

    def __init__(self, credentials_json: str) -> None:
        """
        Inicializa la conexión a BigQuery con las credenciales proporcionadas.
        
        Args:
            credentials_json (str): Credenciales de servicio en formato JSON para autenticar con BigQuery.
        """
        credentials = self.get_credentials(credentials_json)
        self.client = bigquery.Client(credentials=credentials, 
                                      project=credentials.project_id)

        
    def create_table(self, table_id: str, schema: list) -> None:
        """
        Crea una nueva tabla en BigQuery.
        
        Args:
            table_id (str): ID completo de la tabla (proyecto.dataset.tabla).
            schema (list): Esquema de la tabla con la definición de columnas.
        """
        table = bigquery.Table(table_id, schema=schema)

    def get_credentials(self, credentials_json: str) -> service_account.Credentials:
        """
        Obtiene las credenciales de servicio a partir del JSON proporcionado.
        
        Args:
            credentials_json (str): Credenciales de servicio en formato JSON.
            
        Returns:
            google.oauth2.service_account.Credentials: Objeto de credenciales para autenticación.
        """
        credentials = service_account.Credentials.from_service_account_info(
            credentials_json, 
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return credentials

    def check_table_exists(self, table_id: str) -> bool:
        """
        Verifica si una tabla existe en BigQuery.
        
        Args:
            table_id (str): ID completo de la tabla a verificar.
            
        Returns:
            bool: True si la tabla existe, False en caso contrario.
        """
        try:
            self.client.get_table(table_id)
            tabla_existe = True
        except NotFound:
            tabla_existe = False
        return tabla_existe

    def job_config(self, schema: list, table_id: str) -> bigquery.LoadJobConfig:
        """
        Configura un trabajo de carga de datos según la existencia de la tabla.
        
        Si la tabla ya existe, configura el trabajo para añadir datos.
        Si la tabla no existe, configura el trabajo para crearla y cargar datos.
        
        Args:
            schema (list): Esquema de la tabla con la definición de columnas.
            table_id (str): ID completo de la tabla.
            
        Returns:
            google.cloud.bigquery.job.LoadJobConfig: Configuración del trabajo de carga.
        """
        if self.check_table_exists(table_id):
            print("La tabla ya existe")
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
        else:
            print("La tabla no existe")
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_EMPTY
            )
        return job_config

    def create_bq_schema_from_json(self, schema_json: list) -> list:
        """
        Crea un esquema de BigQuery a partir de una definición en formato JSON.
        
        Args:
            schema_json (list): Lista de diccionarios con la definición de columnas.
            
        Returns:
            list: Lista de objetos SchemaField de BigQuery.
        """
        schema = []
        
        for field in schema_json:
            schema.append(
                bigquery.SchemaField(
                    name=field['name'],
                    field_type=field['type'],
                    mode=field['mode'],
                    description=field.get('description', None)
                )
            )
        
        return schema
    
    def load_data_to_bigquery(self, data: pd.DataFrame, table: Table) -> None:
        """
        Carga datos desde un DataFrame a una tabla de BigQuery.
        
        Si el dataset no existe, lo crea antes de cargar los datos.
        
        Args:
            data (pd.DataFrame): DataFrame con los datos a cargar.
            table (Table): Objeto Table con la información de la tabla destino.
        """
        dataset_id = table.get_dataset_id()
        table_id = table.get_table_id()
        try:
            self.client.get_dataset(dataset_id)
        except NotFound:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            dataset = self.client.create_dataset(dataset)
        job_config = self.job_config(table.get_table_schema(), table_id)
        load_job = self.client.load_table_from_dataframe(data, table_id, job_config=job_config)
        load_job.result()


