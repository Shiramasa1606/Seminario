import os
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional
from neo4j import GraphDatabase, Driver, ManagedTransaction

# ==========================
# Cargar variables de entorno
# ==========================
load_dotenv()
NEO4J_URI: Optional[str] = os.getenv("NEO4J_URI")
NEO4J_USER: Optional[str] = os.getenv("NEO4J_USER")
NEO4J_PASSWORD: Optional[str] = os.getenv("NEO4J_PASSWORD")
BASE_PATH: Optional[str] = os.getenv("BASE_PATH")

if not NEO4J_URI or not NEO4J_USER or not NEO4J_PASSWORD:
    raise ValueError("❌ Variables de entorno para Neo4j no encontradas.")

if not BASE_PATH:
    raise ValueError("❌ BASE_PATH no definida en variables de entorno.")

# ==========================
# Conexión con Neo4j
# ==========================
driver: Driver = GraphDatabase.driver(
    NEO4J_URI.strip(), auth=(NEO4J_USER.strip(), NEO4J_PASSWORD.strip())
)

# ==========================
# Funciones para insertar nodos
# ==========================
def insertar_cuestionario(tx: ManagedTransaction, unidad: str, nombre_archivo: str) -> None:
    nombre_sin_csv = Path(nombre_archivo).stem  # Elimina solo .csv
    tx.run(
        """
        MERGE (u:Unidad {nombre: $unidad})
        MERGE (c:Cuestionario {nombre: $nombre})
        MERGE (u)-[:TIENE_CUESTIONARIO]->(c)
        """,
        {"unidad": unidad, "nombre": nombre_sin_csv}
    )
    print(f"✅ Cuestionario insertado: {nombre_sin_csv} en {unidad}")


def insertar_ayudantia(tx: ManagedTransaction, unidad: str, nombre_archivo: str) -> None:
    nombre_sin_csv = Path(nombre_archivo).stem  # Elimina solo .csv
    tx.run(
        """
        MERGE (u:Unidad {nombre: $unidad})
        MERGE (a:Ayudantia {nombre: $nombre})
        MERGE (u)-[:TIENE_AYUDANTIA]->(a)
        """,
        {"unidad": unidad, "nombre": nombre_sin_csv}
    )
    print(f"✅ Ayudantía insertada: {nombre_sin_csv} en {unidad}")

# ==========================
# Función para recorrer carpetas
# ==========================
def procesar_cuestionarios_y_ayudantias(base_path: str) -> None:
    base_path_obj = Path(base_path)
    if not base_path_obj.exists() or not base_path_obj.is_dir():
        raise FileNotFoundError(f"❌ No existe la ruta base: {base_path}")

    with driver.session() as session:
        for unidad in os.listdir(base_path_obj):
            unidad_path = base_path_obj / unidad
            if not unidad_path.is_dir():
                continue
            if unidad.lower() == "alumnos":  # Ignorar carpeta de alumnos
                continue

            print(f"📁 Procesando unidad: {unidad}")

            # Carpeta Cuestionarios
            cuestionarios_folder = unidad_path / "Cuestionarios"
            if cuestionarios_folder.exists() and cuestionarios_folder.is_dir():
                for archivo in os.listdir(cuestionarios_folder):
                    if archivo.lower().endswith(".csv"):
                        session.execute_write(insertar_cuestionario, unidad, archivo)
            else:
                print(f"⚠️ Carpeta Cuestionarios no encontrada en {unidad_path}")

            # Carpeta Ayudantías
            ayudantias_folder = unidad_path / "Ayudantías"
            if ayudantias_folder.exists() and ayudantias_folder.is_dir():
                for archivo in os.listdir(ayudantias_folder):
                    if archivo.lower().endswith(".csv"):
                        session.execute_write(insertar_ayudantia, unidad, archivo)
            else:
                print(f"⚠️ Carpeta Ayudantías no encontrada en {unidad_path}")

# ==========================
# Main
# ==========================
if __name__ == "__main__":
    try:
        procesar_cuestionarios_y_ayudantias(BASE_PATH)
        print("✅ Inserción de cuestionarios y ayudantías completada")
    finally:
        driver.close()
