"""
Módulo de conexión a Neo4j.

Este archivo maneja la creación y administración del driver para conectarse
a una base de datos Neo4j utilizando las credenciales almacenadas en variables
de entorno.

Variables de entorno necesarias (definir en .env o sistema):
    - NEO4J_URI: URI de conexión
    - NEO4J_USER: Usuario de Neo4j
    - NEO4J_PASSWORD: Contraseña de Neo4j

Funciones principales:
    - obtener_driver(): Devuelve una instancia singleton del driver.
    - driver_context(): Proporciona un contexto seguro para usar el driver,
      cerrándolo automáticamente al salir del bloque `with`.

Buenas prácticas:
    - Usar `driver_context()` en scripts pequeños o de prueba.
    - Usar `obtener_driver()` en aplicaciones grandes (Express, FastAPI, etc.)
      donde el driver debe vivir mientras la app esté corriendo.
"""

import os
import logging
from neo4j import GraphDatabase, Driver
from dotenv import load_dotenv
from contextlib import contextmanager
from typing import Generator, Optional

# Setup logging
logger = logging.getLogger(__name__)

# === Cargar variables de entorno ===
load_dotenv()

# Get and validate environment variables
URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PASSWORD = os.getenv("NEO4J_PASSWORD")

# Validación temprana: si faltan credenciales, detener ejecución
if not URI or not USER or not PASSWORD:
    error_msg = (
        "❌ Faltan variables de entorno para Neo4j: "
        "NEO4J_URI, NEO4J_USER o NEO4J_PASSWORD"
    )
    logger.error(error_msg)
    raise EnvironmentError(error_msg)

# Driver singleton (se inicializa solo una vez)
_driver: Optional[Driver] = None


def obtener_driver() -> Driver:
    """
    Devuelve una instancia global (singleton) del driver de Neo4j.

    Si el driver aún no fue creado, se inicializa usando las variables
    de entorno cargadas. En posteriores llamadas, devuelve el mismo driver.

    Returns:
        neo4j.Driver: Instancia del driver de Neo4j.
    
    Raises:
        Exception: Si la conexión falla durante la creación del driver.
    """
    global _driver
    if _driver is None:
        try:
            _driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))  # type: ignore
            # Verificación opcional de conexión
            _driver.verify_connectivity()
            logger.info("✅ Driver de Neo4j creado y conectado exitosamente.")
        except Exception as e:
            logger.error(f"❌ Error al crear el driver de Neo4j: {e}")
            _driver = None
            raise
    return _driver


@contextmanager
def driver_context() -> Generator[Driver, None, None]:
    """
    Context manager para manejar la conexión con Neo4j de forma segura.

    Crea un driver temporal y lo cierra automáticamente al salir del bloque `with`.

    Uso:
        with driver_context() as driver:
            with driver.session() as session:
                result = session.run("MATCH (n) RETURN n LIMIT 5")
                for row in result:
                    print(row)
                    
    Raises:
        Exception: Si la creación del driver falla.
    """
    driver: Optional[Driver] = None
    try:
        driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))  # type: ignore
        driver.verify_connectivity()
        logger.debug("Driver temporal de Neo4j creado para contexto.")
        yield driver
    except Exception as e:
        logger.error(f"Error en driver_context: {e}")
        raise
    finally:
        if driver is not None:
            driver.close()
            logger.debug("Driver temporal de Neo4j cerrado.")


def cerrar_driver() -> None:
    """
    Cierra manualmente el driver singleton, si existe.

    Recomendado al finalizar la aplicación para liberar recursos.
    """
    global _driver
    if _driver is not None:
        _driver.close()
        _driver = None
        logger.info("Driver de Neo4j cerrado exitosamente.")


def verificar_conexion() -> bool:
    """
    Verifica que la conexión a Neo4j esté funcionando.
    
    Returns:
        bool: True si la conexión es exitosa, False en caso contrario.
    """
    try:
        driver = obtener_driver()
        with driver.session() as session:
            result = session.run("RETURN 1 as test")
            single_result = result.single()
            return single_result is not None and single_result["test"] == 1
    except Exception as e:
        logger.error(f"Error verificando conexión: {e}")
        return False