# Neo4J/Inserts/insertarCuestionariosAyudantias.py

from pathlib import Path
from typing import Union
from neo4j import Driver, ManagedTransaction
import re

# ==========================
# Función: insertar un Cuestionario
# ==========================
def insertar_cuestionario(tx: ManagedTransaction, unidad: str, nombre_archivo: str) -> None:
    """
    Inserta (MERGE) un nodo :Cuestionario con su relación a :Unidad.
    nombre_archivo puede ser el nombre con extensión (archivo.csv) o solo el nombre.
    Esta función limpia sufijos tipo '-calificaciones'.
    """
    nombre_raw = Path(nombre_archivo).stem  # elimina extensión si la tiene
    # quitar sufijo "-calificaciones" (insensible a mayúsc/minúsc)
    nombre_limpio = re.sub(r'(?i)\s*-calificaciones\s*$', '', nombre_raw).strip()

    tx.run(
        """
        MERGE (u:Unidad {nombre: $unidad})
        MERGE (c:Cuestionario {nombre: $nombre})
        MERGE (u)-[:TIENE_CUESTIONARIO]->(c)
        """,
        unidad=unidad,
        nombre=nombre_limpio,
    )
    print(f"✅ Cuestionario insertado/validado: {nombre_limpio} en {unidad}")


# ==========================
# Función: insertar una Ayudantía
# ==========================
def insertar_ayudantia(tx: ManagedTransaction, unidad: str, nombre_archivo: str) -> None:
    """
    Inserta (MERGE) un nodo :Ayudantia con su relación a :Unidad.
    Nombre limpiado igual que en insertar_cuestionario.
    """
    nombre_raw = Path(nombre_archivo).stem
    nombre_limpio = re.sub(r'(?i)\s*-calificaciones\s*$', '', nombre_raw).strip()

    tx.run(
        """
        MERGE (u:Unidad {nombre: $unidad})
        MERGE (a:Ayudantia {nombre: $nombre})
        MERGE (u)-[:TIENE_AYUDANTIA]->(a)
        """,
        unidad=unidad,
        nombre=nombre_limpio,
    )
    print(f"✅ Ayudantía insertada/validada: {nombre_limpio} en {unidad}")


# ==========================
# Función: procesar carpetas de Unidades y llamar a los inserts
# ==========================
def procesar_cuestionarios_y_ayudantias(driver: Driver, base_path: Union[str, Path]) -> None:
    """
    Recorre las carpetas bajo base_path que comiencen con 'Unidad' (ignora 'Alumnos'),
    busca carpetas 'Cuestionarios' y 'Ayudantías' y para cada .csv llama al insert
    correspondiente usando session.execute_write.
    - driver: instancia neo4j.Driver provista por el main.
    - base_path: Path o str a la carpeta que contiene 'Unidad 1', 'Unidad 2', ...
    """
    base = Path(base_path)
    if not base.exists() or not base.is_dir():
        raise FileNotFoundError(f"La ruta base no es válida: {base}")

    with driver.session() as session:
        for carpeta in sorted(base.iterdir()):
            if not carpeta.is_dir():
                continue
            # ignorar la carpeta de alumnos si existe
            if carpeta.name.lower() == "alumnos":
                continue
            # opcional: solo procesar carpetas que empiecen con "unidad"
            if not carpeta.name.lower().startswith("unidad"):
                print(f"⏭️ Carpeta ignorada (no 'Unidad'): {carpeta.name}")
                continue

            unidad_nombre = carpeta.name
            print(f"📁 Procesando unidad: {unidad_nombre}")

            # Cuestionarios
            cuestionarios_dir = carpeta / "Cuestionarios"
            if cuestionarios_dir.exists() and cuestionarios_dir.is_dir():
                for archivo in sorted(cuestionarios_dir.iterdir()):
                    if archivo.is_file() and archivo.suffix.lower() == ".csv":
                        print(f"   📄 Procesando cuestionario: {archivo.name}")
                        # pasamos el nombre del archivo (archivo.name) para que la función lo limpie
                        session.execute_write(insertar_cuestionario, unidad_nombre, archivo.name)
            else:
                print(f"⚠️ Carpeta 'Cuestionarios' no encontrada en {carpeta}")

            # Ayudantías
            ayudantias_dir = carpeta / "Ayudantías"
            if ayudantias_dir.exists() and ayudantias_dir.is_dir():
                for archivo in sorted(ayudantias_dir.iterdir()):
                    if archivo.is_file() and archivo.suffix.lower() == ".csv":
                        print(f"   📄 Procesando ayudantía: {archivo.name}")
                        session.execute_write(insertar_ayudantia, unidad_nombre, archivo.name)
            else:
                print(f"⚠️ Carpeta 'Ayudantías' no encontrada en {carpeta}")
