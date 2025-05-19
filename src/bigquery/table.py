"""
Módulo para la gestión de tablas en BigQuery.

Este módulo proporciona una clase para manejar la información y estructura
de las tablas en BigQuery, acceder a sus esquemas, configuraciones y
realizar transformaciones sobre sus datos.

La clase Table permite inicializar una tabla con parámetros opcionales
como table_id, schema y location, o cargarlos desde el archivo de
configuración si no se proporcionan.
"""

import pandas as pd
from src.config.config import cfg_item

class Table:
    """
    Clase para representar y gestionar tablas de BigQuery.
    
    Esta clase permite acceder a la información de una tabla específica,
    obtener su esquema, configuraciones, realizar transformaciones de datos
    y otras operaciones relacionadas con la estructura de la tabla.
    
    La clase puede inicializarse con parámetros opcionales para table_id,
    schema y location, o cargar estos valores desde el archivo de configuración
    si no se proporcionan.
    """

    def __init__(self, table_name: str, 
                 table_id: str = None, 
                 schema: list = None,
                 location: str = None) -> None:
        """
        Inicializa una instancia de tabla con el nombre especificado.
        
        Args:
            table_name (str): Nombre de la tabla a gestionar.
            table_id (str, opcional): ID completo de la tabla (proyecto.dataset.tabla).
                Si no se proporciona, se cargará desde la configuración.
            schema (list, opcional): Esquema de la tabla con la definición de columnas.
                Si no se proporciona, se cargará desde la configuración.
            location (str, opcional): Ubicación geográfica de la tabla (ej. 'US').
                Si no se proporciona, se cargará desde la configuración.
        """
        self.table_name = table_name
        self.table_id = table_id
        self.schema = schema
        self.location = location
        self.attr_columns()
    
    def attr_columns(self) -> None:
        """
        Crea atributos dinámicos para cada columna de la tabla.
        
        Lee el esquema de la tabla y asigna cada nombre de columna
        como un atributo de la instancia para facilitar su referencia.
        """
        schema = self.get_table_schema()
        for column in schema:
            column_name = column['name']
            setattr(self, column_name, column_name)

    def get_table_id(self) -> str:
        """
        Obtiene el ID completo de la tabla.
        
        Si el ID no fue proporcionado en la inicialización, lo carga
        desde la configuración.
        
        Returns:
            str: ID completo de la tabla (proyecto.dataset.tabla).
        """
        if self.table_id is None:
            self.table_id = cfg_item('data', 'tables', self.table_name, 'table_id')
        return self.table_id
    
    def get_dataset_id(self) -> str:
        """
        Obtiene el ID del dataset.
        
        Si el ID del dataset no fue proporcionado en la inicialización,
        lo carga desde la configuración.
        
        Returns:
            str: ID del dataset que contiene la tabla.
        """
        if self.dataset_id is None:
            self.dataset_id = cfg_item('data', 'dataset_id')
        return self.dataset_id

    def get_table_schema(self) -> list:
        """
        Obtiene el esquema de la tabla.
        
        Si el esquema no fue proporcionado en la inicialización,
        lo carga desde la configuración.
        
        Returns:
            list: Lista de diccionarios con la definición de columnas.
        """
        if self.schema is None:
            self.schema = cfg_item('data', 'tables', self.table_name, 'schema')
        return self.schema
    
    def get_location(self) -> str:
        """
        Obtiene la ubicación geográfica de la tabla.
        
        Si la ubicación no fue proporcionada en la inicialización,
        la carga desde la configuración.
        
        Returns:
            str: Ubicación geográfica donde se almacena la tabla (ej. 'US').
        """
        if self.location is None:
            self.location = cfg_item('data', 'location')
        return self.location
    
    def get_date_columns(self) -> list:
        """
        Identifica y obtiene las columnas de tipo fecha en la tabla.
        
        Returns:
            list: Lista con los nombres de las columnas de tipo fecha.
        """
        date_columns = []
        for column in self.get_table_schema():
            if column['type'] == 'DATE':
                date_columns.append(column['name'])
        return date_columns
    
    def transform_date_time(self, df: pd.DataFrame, 
                            date_columns: list) -> pd.DataFrame:
        """
        Transforma las columnas de fecha en formato datetime.
        
        Args:
            df (pd.DataFrame): DataFrame con los datos a transformar.
            date_columns (list): Lista de nombres de columnas de fecha.
            
        Returns:
            pd.DataFrame: DataFrame con las columnas de fecha transformadas.
        """
        for column in date_columns:
            df[column] = pd.to_datetime(df[column])
        return df


