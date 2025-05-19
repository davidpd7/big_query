"""
Módulo de configuración para la aplicación.

Este módulo proporciona acceso a la configuración de la aplicación
a través de un patrón singleton, permitiendo cargar configuraciones
desde un archivo JSON y acceder a sus valores de forma estructurada.
"""

import os
import json

def cfg_item(*items: str) -> any:
    """
    Accede a elementos anidados en la configuración.
    
    Esta función permite navegar por la estructura jerárquica de la configuración
    utilizando una secuencia de claves como ruta.
    
    Args:
        *items: Secuencia variable de claves que representan la ruta al elemento deseado.
        
    Returns:
        El valor del elemento de configuración en la ruta especificada.
    """
    data = Config.instance().data
    for key in items:
        data = data[key]
    return data

class Config:
    """
    Clase singleton para gestionar la configuración de la aplicación.
    
    Esta clase implementa el patrón singleton para garantizar que solo exista
    una instancia de la configuración en toda la aplicación. Carga los datos
    de configuración desde un archivo JSON y proporciona métodos para acceder
    a ellos.
    """

    __instance = None

    @staticmethod
    def instance():
        """
        Obtiene la instancia única de la configuración.
        
        Si la instancia no existe, la crea automáticamente.
        
        Returns:
            Config: La instancia única de la configuración.
        """
        if Config.__instance is None:
            Config()
        return Config.__instance
    
    def __init__(self, config_json_filename: str = "config.json") -> None:
        """
        Inicializa la configuración cargando los datos desde el archivo JSON.
        
        Args:
            config_json_filename (str, opcional): Nombre del archivo de configuración.
                Por defecto es "config.json".
                
        Raises:
            Exception: Si se intenta crear más de una instancia.
        """
        self.__config_dir = os.path.dirname(__file__)
        self.__config_json_filename = config_json_filename

        if Config.__instance is None:
            Config.__instance = self
            config_path = os.path.join(self.__config_dir, self.__config_json_filename)
            with open(config_path) as file:
                self.data = json.load(file)
            self.__debug = False
        else:
            raise Exception("Config solo se puede instanciar una vez")

    @property
    def debug(self) -> bool:
        """
        Obtiene el estado de depuración.
        
        Returns:
            bool: El estado actual del modo de depuración.
        """
        return self.__debug

    @debug.setter
    def debug(self, value: bool) -> None:
        """
        Establece el estado de depuración.
        
        Args:
            value (bool): El nuevo estado para el modo de depuración.
        """
        self.__debug = value
    