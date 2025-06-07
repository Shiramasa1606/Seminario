import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()  # carga las variables de entorno del archivo .env

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "INF4556-5")

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

def obtener_driver():
    return driver
