import os
from neo4j import GraphDatabase, Driver, ManagedTransaction
from dotenv import load_dotenv



# ===================== Conexión =====================
def obtener_driver() -> Driver:
    load_dotenv()
    NEO4J_URI: str = os.getenv("NEO4J_URI", "")
    NEO4J_USER: str = os.getenv("NEO4J_USER", "")
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "")
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# ===================== Funciones =====================
def insertar_unidad(tx: ManagedTransaction, nombre_unidad: str) -> None:
    tx.run("""
        MERGE (u:Unidad {nombre: $nombre})
    """, nombre=nombre_unidad)
    print(f"✅ Unidad insertada/validada: {nombre_unidad}")

def insertar_rap(tx: ManagedTransaction, nombre_unidad: str, nombre_rap: str) -> None:
    tx.run("""
        MATCH (u:Unidad {nombre: $unidad})
        MERGE (r:RAP {nombre: $rap})
        MERGE (u)-[:TIENE_RAP]->(r)
    """, unidad=nombre_unidad, rap=nombre_rap)
    print(f"   📘 RAP insertado/validado en {nombre_unidad}: {nombre_rap}")

def procesar_unidades_y_raps(base_path: str) -> None:
    driver: Driver = obtener_driver()
    with driver.session() as session:
        for carpeta in os.listdir(base_path):
            carpeta_path = os.path.join(base_path, carpeta)

            # Filtrar: solo aceptar carpetas que empiecen con "Unidad"
            if os.path.isdir(carpeta_path) and carpeta.lower().startswith("unidad"):
                # Insertar unidad
                session.execute_write(insertar_unidad, carpeta)

                # Buscar carpeta RAP dentro de la unidad
                rap_folder = os.path.join(carpeta_path, "RAP")
                if os.path.isdir(rap_folder):
                    for archivo in os.listdir(rap_folder):
                        rap_name, _ = os.path.splitext(archivo)
                        session.execute_write(insertar_rap, carpeta, rap_name)
                else:
                    print(f"⚠️ Carpeta RAP no encontrada en {carpeta_path}")
            else:
                print(f"⏭️ Carpeta ignorada: {carpeta}")
    driver.close()

# ===================== Main =====================
if __name__ == "__main__":
    BASE_PATH: str = os.getenv("BASE_PATH", "")
    procesar_unidades_y_raps(BASE_PATH)