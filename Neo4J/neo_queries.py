# Neo4J/neo_queries.py
from typing import List, Dict, Any, Optional
from neo4j import Driver
from Neo4J.conn import obtener_driver


# ------------------------
# Consultas Cypher centralizadas
# ------------------------

def fetch_alumnos() -> List[Dict[str, str]]:
    """
    Devuelve la lista de alumnos con 'correo' y 'nombre' (ordenados por nombre).
    """
    driver: Driver = obtener_driver()
    cypher: str = """
    MATCH (a:Alumno)
    RETURN a.correo AS correo, a.nombre AS nombre
    ORDER BY a.nombre
    """
    try:
        with driver.session() as session:
            result = session.run(cypher)
            alumnos: List[Dict[str, str]] = [
                {"correo": str(record["correo"]), "nombre": str(record["nombre"])}
                for record in result
                if record.get("correo") and record.get("nombre")
            ]
            return alumnos
    finally:
        driver.close()


def fetch_progreso_alumno_robust(correo: str) -> List[Dict[str, Any]]:
    """
    Versión más robusta con mejor manejo de tipos.
    """
    driver: Driver = obtener_driver()
    cypher: str = """
    MATCH (a:Alumno {correo: $correo})-[r]->(act)
    WHERE type(r) IN ["Intento","Completado","Perfecto"]
    RETURN labels(act) AS labels, act.nombre AS nombre,
           type(r) AS estado_relacion,
           r.start AS start, r.end AS end, r.duration_seconds AS duration_seconds,
           r.score AS score, r.estado AS estado_raw
    """
    try:
        with driver.session() as session:
            result = session.run(cypher, correo=correo)
            progreso: List[Dict[str, Any]] = []

            for record in result:
                # More robust type handling
                raw_labels = record.get("labels")
                if raw_labels is None:
                    labels: List[str] = []
                else:
                    labels = [str(label) for label in raw_labels]
                
                tipo: str = labels[0] if labels else "Desconocido"

                progreso.append({
                    "tipo": tipo,
                    "nombre": record.get("nombre"),
                    "estado": record.get("estado_relacion"),
                    "start": record.get("start"),
                    "end": record.get("end"),
                    "duration_seconds": record.get("duration_seconds"),
                    "score": record.get("score"),
                    "estado_raw": record.get("estado_raw")
                })
            return progreso
    finally:
        driver.close()


def fetch_siguiente_por_avance(correo: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene una actividad candidata cuando el alumno ya tiene 'Perfecto'
    y se busca la siguiente actividad en la misma unidad o RAP que no esté completada.
    Retorna un dict con 'tipo' y 'nombre', o None si no hay sugerencias.
    """
    driver: Driver = obtener_driver()
    cypher: str = """
    MATCH (a:Alumno {correo:$correo})-[:Perfecto]->(act)
          <-[:TIENE_RAP|:TIENE_CUESTIONARIO|:TIENE_AYUDANTIA]-(unidad:Unidad)
    MATCH (unidad)-[:TIENE_RAP|:TIENE_CUESTIONARIO|:TIENE_AYUDANTIA]->(siguiente)
    WHERE NOT (a)-[:Completado|:Perfecto]->(siguiente)
    RETURN labels(siguiente) AS labels, siguiente.nombre AS nombre
    LIMIT 1
    """
    try:
        with driver.session() as session:
            record = session.run(cypher, correo=correo).single()
            if not record:
                return None

            # Fix: Add explicit type annotations
            labels: List[str] = list(record["labels"] or [])
            tipo: str = labels[0] if labels else "Desconocido"

            return {"tipo": tipo, "nombre": record.get("nombre")}
    finally:
        driver.close()