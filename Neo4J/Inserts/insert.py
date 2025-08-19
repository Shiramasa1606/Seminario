import os
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
from pandas import DataFrame
from neo4j import GraphDatabase, Driver, ManagedTransaction

# ==========================
# Cargar variables de entorno
# ==========================
load_dotenv()
NEO4J_URI: str = os.getenv("NEO4J_URI", "")
NEO4J_USER: str = os.getenv("NEO4J_USER", "")
NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "")

# ==========================
# Conexión con Neo4j
# ==========================
driver: Driver = GraphDatabase.driver(
    NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
)

# ==========================
# Función: limpiar la BD
# ==========================
def limpiar_bd(tx: ManagedTransaction) -> None:
    tx.run("MATCH (n) DETACH DELETE n")

# ==========================
# Función: insertar alumnos
# ==========================
def insertar_alumnos(tx: ManagedTransaction, alumnos: DataFrame) -> None:
    for idx, row in alumnos.iterrows():
        tx.run(
            """
            CREATE (a:Alumno {
                id: $id,
                nombre: $nombre,
                apellido: $apellido,
                correo: $correo
            })
            """,
            {
                "id": str(idx),  # usar índice como ID único
                "nombre": str(row["Nombre"]),
                "apellido": str(row["Apellido(s)"]),
                "correo": str(row["Dirección de correo"]),
            },
        )

# ==========================
# Carga inicial de datos
# ==========================
def carga_inicial() -> None:
    # Ruta actualizada del CSV
    ruta_csv = Path(__file__).parent.parent / "Resultados Cuestionarios Algoritmos, Paralelo 3" / "Alumnos" / "Alumnos_Paralelo_03.csv"

    if not ruta_csv.exists():
        raise FileNotFoundError(f"No se encontró el archivo CSV en: {ruta_csv}")

    alumnos: DataFrame = pd.read_csv(ruta_csv) # type: ignore

    with driver.session() as session:
        session.execute_write(limpiar_bd)
        session.execute_write(insertar_alumnos, alumnos)

    print("✅ Carga inicial completada")

# ==========================
# Main
# ==========================
if __name__ == "__main__":
    carga_inicial()
    driver.close()
