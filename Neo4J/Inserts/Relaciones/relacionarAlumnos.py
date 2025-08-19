# Neo4J\Inserts\Relaciones\relacionarAlumnos.py

import os
from pathlib import Path
from typing import Literal
import pandas as pd
from neo4j import GraphDatabase, Driver, ManagedTransaction, Session
from dotenv import load_dotenv

TipoRelacion = Literal["Intento", "Completado", "Perfecto"]

def obtener_driver() -> Driver:
    load_dotenv()
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")

    if not uri or not user or not password:
        raise RuntimeError(
            "Faltan variables de entorno: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD."
        )

    return GraphDatabase.driver(uri, auth=(user, password))

# --------------------------
# Relaciones con verificación de existencia
# --------------------------
def relacionar_alumno_cuestionario(tx: ManagedTransaction, alumno_correo: str, cuestionario_nombre: str, tipo_relacion: TipoRelacion):
    # Verificar existencia de alumno y cuestionario
    alumno = tx.run("MATCH (al:Alumno {correo: $correo}) RETURN al", correo=alumno_correo).single()
    cuestionario = tx.run("MATCH (q:Cuestionario {nombre: $nombre}) RETURN q", nombre=cuestionario_nombre).single()

    if not alumno:
        print(f"⚠️ Alumno no encontrado: {alumno_correo}")
    if not cuestionario:
        print(f"⚠️ Cuestionario no encontrado: {cuestionario_nombre}")
    if not alumno or not cuestionario:
        return

    print(f"ℹ️ Alumno y cuestionario encontrados: {alumno_correo} -> {cuestionario_nombre}, creando relación...")

    query = f"""
        MATCH (al:Alumno {{correo:$correo}})
        MATCH (q:Cuestionario {{nombre:$nombre}})
        MERGE (al)-[r:{tipo_relacion}]->(q)
        RETURN r
    """
    result = tx.run(query, correo=alumno_correo, nombre=cuestionario_nombre)
    if result.single():
        print(f"✅ Relación {tipo_relacion} insertada correctamente: {alumno_correo} -> {cuestionario_nombre}")
    else:
        print(f"⚠️ Error al insertar relación: {alumno_correo} -> {cuestionario_nombre}")


def relacionar_alumno_ayudantia(tx: ManagedTransaction, alumno_correo: str, ayudantia_nombre: str, tipo_relacion: TipoRelacion):
    # Verificar existencia de alumno y ayudantia
    alumno = tx.run("MATCH (al:Alumno {correo: $correo}) RETURN al", correo=alumno_correo).single()
    ayudantia = tx.run("MATCH (a:Ayudantia {nombre: $nombre}) RETURN a", nombre=ayudantia_nombre).single()

    if not alumno:
        print(f"⚠️ Alumno no encontrado: {alumno_correo}")
    if not ayudantia:
        print(f"⚠️ Ayudantia no encontrada: {ayudantia_nombre}")
    if not alumno or not ayudantia:
        return

    print(f"ℹ️ Alumno y ayudantia encontrados: {alumno_correo} -> {ayudantia_nombre}, creando relación...")

    query = f"""
        MATCH (al:Alumno {{correo:$correo}})
        MATCH (a:Ayudantia {{nombre:$nombre}})
        MERGE (al)-[r:{tipo_relacion}]->(a)
        RETURN r
    """
    result = tx.run(query, correo=alumno_correo, nombre=ayudantia_nombre)
    if result.single():
        print(f"✅ Relación {tipo_relacion} insertada correctamente: {alumno_correo} -> {ayudantia_nombre}")
    else:
        print(f"⚠️ Error al insertar relación: {alumno_correo} -> {ayudantia_nombre}")


# --------------------------
# Obtener lista de alumnos
# --------------------------
def obtener_lista_alumnos(session: Session) -> list[str]:
    result = session.run("MATCH (al:Alumno) RETURN al.correo AS correo")
    return [row["correo"].strip().lower() for row in result]


# --------------------------
# Procesar CSV
# --------------------------
def procesar_csv(session: Session, recurso_path: Path, tipo_recurso: Literal["Cuestionario", "Ayudantia"]) -> None:
    df = pd.read_csv(recurso_path)  # type: ignore

    columna_correo = next((c for c in df.columns if "correo" in c.lower()), None)
    if columna_correo is None:
        raise ValueError(f"No se encontró la columna de correo en {recurso_path}")

    csv_correos = {str(row[columna_correo]).strip().lower() for _, row in df.iterrows() if str(row[columna_correo]).strip()}
    alumnos_bd = obtener_lista_alumnos(session)
    recurso_nombre = recurso_path.stem
    if recurso_nombre.lower().endswith("-calificaciones"):
        recurso_nombre = recurso_nombre[:-len("-calificaciones")]



    for correo in alumnos_bd:
        if correo not in csv_correos:
            print(f"⚠️ Alumno no presente en CSV, se omite: {correo}")
            continue

        row = df[df[columna_correo].str.strip().str.lower() == correo].iloc[0]
        estado = str(row.get("Estado", "")).strip()
        calificacion = str(row.get('Calificación/100,00', "")).strip()

        tipo_relacion: TipoRelacion = "Intento"
        if estado.lower() == "finalizado":
            tipo_relacion = "Completado"
            if calificacion == "100,00":
                tipo_relacion = "Perfecto"

        if tipo_recurso == "Cuestionario":
            session.execute_write(relacionar_alumno_cuestionario, correo, recurso_nombre, tipo_relacion)
        else:
            session.execute_write(relacionar_alumno_ayudantia, correo, recurso_nombre, tipo_relacion)


# --------------------------
# Procesar unidades
# --------------------------
def procesar_unidades(BASE_PATH: Path) -> None:
    driver = obtener_driver()
    try:
        with driver.session() as session:
            for unidad_dir in BASE_PATH.iterdir():
                if not unidad_dir.is_dir() or unidad_dir.name.lower() == "alumnos":
                    continue

                print(f"📁 Procesando unidad: {unidad_dir.name}")

                cuestionarios_path = unidad_dir / "Cuestionarios"
                if cuestionarios_path.exists() and cuestionarios_path.is_dir():
                    for archivo in cuestionarios_path.iterdir():
                        if archivo.is_file() and archivo.suffix.lower() == ".csv":
                            print(f"   📄 Procesando cuestionario: {archivo.name}")
                            procesar_csv(session, archivo, "Cuestionario")

                ayudantias_path = unidad_dir / "Ayudantías"
                if ayudantias_path.exists() and ayudantias_path.is_dir():
                    for archivo in ayudantias_path.iterdir():
                        if archivo.is_file() and archivo.suffix.lower() == ".csv":
                            print(f"   📄 Procesando ayudantía: {archivo.name}")
                            procesar_csv(session, archivo, "Ayudantia")
    finally:
        driver.close()


# --------------------------
# Main
# --------------------------
if __name__ == "__main__":
    load_dotenv()
    base_env_path = os.getenv("BASE_PATH")
    if not base_env_path:
        raise RuntimeError("Falta la variable de entorno BASE_PATH")

    BASE_PATH = Path(base_env_path)
    print(f"📁 Ruta base desde variable de entorno: {BASE_PATH}")
    procesar_unidades(BASE_PATH)
