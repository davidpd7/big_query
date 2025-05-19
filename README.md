# BigQuery Integration

Paquete para la integración con Google BigQuery que proporciona funcionalidades para conectarse a BigQuery, gestionar tablas, cargar datos y realizar consultas.

## Requisitos

- Python 3.7 o superior
- Credenciales de servicio de Google Cloud (archivo JSON)

## Instalación

### Opción 1: Instalación desde el repositorio

```bash
# Clonar el repositorio
git clone https://github.com/davidpd997/big_query.git
cd big_query

# Instalar el paquete
pip install -e https://github.com/davidpd997/big_query.git
```

### Opción 2: Instalación con pip

```bash
pip install -r requirements.txt
```

## Configuración

El paquete utiliza un archivo de configuración `config.json` ubicado en `src/config/`. Este archivo contiene la configuración de conexión a BigQuery, esquemas de tablas y otros parámetros.

## Uso básico

```python
# Importar las clases necesarias
from src.bigquery import BigQuery
from src.table import Table

# Cargar las credenciales de servicio (como diccionario JSON)
import json
with open('ruta/a/credenciales.json', 'r') as f:
    credentials_json = json.load(f)

# Crear una instancia de BigQuery
bq = BigQuery(credentials_json)

# Utilizar una tabla específica
table = Table('options')

# Cargar datos en la tabla
import pandas as pd
data = pd.DataFrame(...)  # Tus datos
bq.load_data_to_bigquery(data, table)
```

## Estructura del proyecto

- `src/`: Código fuente del paquete
  - `__init__.py`: Inicialización del paquete
  - `bigquery.py`: Clase principal para conexión con BigQuery
  - `table.py`: Clase para gestionar tablas
  - `config/`: Módulo de configuración
    - `__init__.py`: Inicialización del módulo
    - `config.py`: Clase para gestionar la configuración
    - `config.json`: Archivo de configuración

## Licencia

Ver archivo LICENSE para más detalles. 