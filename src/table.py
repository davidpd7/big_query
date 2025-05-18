"""
Módulo para la gestión de tablas en BigQuery.

Este módulo proporciona una clase para manejar la información y estructura
de las tablas en BigQuery, acceder a sus esquemas, configuraciones y
realizar transformaciones sobre sus datos.
"""

import pandas as pd
from src.config.config import cfg_item

class Table:
    """
    Clase para representar y gestionar tablas de BigQuery.
    
    Esta clase permite acceder a la información de una tabla específica,
    obtener su esquema, configuraciones, realizar transformaciones de datos
    y otras operaciones relacionadas con la estructura de la tabla.
    """

    def __init__(self, table_name: str) -> None:
        """
        Inicializa una instancia de tabla con el nombre especificado.
        
        Args:
            table_name (str): Nombre de la tabla a gestionar.
        """
        self.table_name = table_name
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
        Obtiene el ID completo de la tabla desde la configuración.
        
        Returns:
            str: ID completo de la tabla (proyecto.dataset.tabla).
        """
        return cfg_item('data', 'tables', self.table_name, 'table_id')
    
    def get_dataset_id(self) -> str:
        """
        Obtiene el ID del dataset desde la configuración.
        
        Returns:
            str: ID del dataset que contiene la tabla.
        """
        return cfg_item('data', 'dataset_id')

    def get_table_schema(self) -> list:
        """
        Obtiene el esquema de la tabla desde la configuración.
        
        Returns:
            list: Lista de diccionarios con la definición de columnas.
        """
        return cfg_item('data', 'tables', self.table_name, 'schema')
    
    def get_data(self) -> dict:
        """
        Obtiene la configuración de datos para la tabla.
        
        Returns:
            dict: Configuración de datos para la tabla.
        """
        return cfg_item('data', 'tables', self.table_name, 'data')

    def get_location(self) -> str:
        """
        Obtiene la ubicación geográfica de la tabla desde la configuración.
        
        Returns:
            str: Ubicación geográfica donde se almacena la tabla (ej. 'US').
        """
        return cfg_item('data', 'location')
    
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
    
    def transform_date_time(self, df: pd.DataFrame, date_columns: list) -> pd.DataFrame:
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


