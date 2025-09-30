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
from neo4j import GraphDatabase, Driver
from dotenv import load_dotenv
from contextlib import contextmanager
from typing import Generator

# === Cargar variables de entorno ===
load_dotenv()

URI = str(os.getenv("NEO4J_URI"))
USER = str(os.getenv("NEO4J_USER"))
PASSWORD = str(os.getenv("NEO4J_PASSWORD"))

# Validación temprana: si faltan credenciales, detener ejecución
if not URI or not USER or not PASSWORD:
    raise EnvironmentError(
        "❌ Faltan variables de entorno para Neo4j: "
        "NEO4J_URI, NEO4J_USER o NEO4J_PASSWORD"
    )

# Driver singleton (se inicializa solo una vez)
_driver: Driver | None = None


def obtener_driver() -> Driver:
    """
    Devuelve una instancia global (singleton) del driver de Neo4j.

    Si el driver aún no fue creado, se inicializa usando las variables
    de entorno cargadas. En posteriores llamadas, devuelve el mismo driver.

    Returns:
        neo4j.Driver: Instancia del driver de Neo4j.
    """
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
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
    """
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    try:
        yield driver
    finally:
        driver.close()


def cerrar_driver() -> None:
    """
    Cierra manualmente el driver singleton, si existe.

    Recomendado al finalizar la aplicación para liberar recursos.
    """
    global _driver
    if _driver is not None:
        _driver.close()
        _driver = None
