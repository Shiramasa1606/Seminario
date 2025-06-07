import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Carga las variables de entorno definidas en el archivo .env
load_dotenv()

# Obtiene las variables de entorno necesarias para conectar a Neo4j
URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PASSWORD = os.getenv("NEO4J_PASSWORD")

def obtener_driver():
    """
    Crea y devuelve una instancia del driver de Neo4j usando
    los parámetros de conexión almacenados en variables de entorno.

    Este driver es utilizado para crear sesiones y ejecutar
    transacciones en la base de datos Neo4j.

    Returns:
        neo4j.Driver: Driver para la conexión con la base Neo4j.
    """
    if not URI or not USER or not PASSWORD:
        raise ValueError("Faltan variables de entorno para Neo4j: URI, USER o PASSWORD")

    return GraphDatabase.driver(URI, auth=(USER, PASSWORD))
