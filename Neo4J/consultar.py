from neo4j import ManagedTransaction
from typing import List, Optional, Mapping, Any

# ===================== Consulta de progreso ===========================

def progreso_alumno(tx: ManagedTransaction, alumno_id: str) -> List[Mapping[str, Any]]:
    """
    Obtiene el estado de avance de un alumno sobre todas las actividades (Cuestionario, RAP, Ayudantía).

    Retorna:
        Lista de dicts con:
            - tipo: tipo de nodo (ej. "Cuestionario", "RAP", "Ayudantia")
            - nombre: nombre de la actividad
            - estado: "Intento", "Completado" o "Perfecto"
    """
    query = """
    MATCH (a:Alumno {id:$alumno_id})-[r]->(act)
    WHERE type(r) IN ["Intento", "Completado", "Perfecto"]
    RETURN labels(act) AS tipo, act.nombre AS nombre, type(r) AS estado
    """
    result = tx.run(query, alumno_id=alumno_id)
    return [dict(record) for record in result]

# ===================== Estrategia de recomendación híbrida ===========================

def recomendar_siguiente(tx: ManagedTransaction, alumno_id: str) -> Optional[Mapping[str, Any]]:
    """
    Genera la recomendación de la siguiente actividad para un alumno según estrategia híbrida:
        - Refuerzo: si tiene actividades iniciadas pero no completadas ("Intento")
        - Mejora: si completó actividades pero no con 100% ("Completado")
        - Avance: si tiene actividades perfectas ("Perfecto"), recomendar siguiente unidad/RAP disponible
    """
    progreso = progreso_alumno(tx, alumno_id)

    # ----- Estrategia refuerzo -----
    intentos = [p for p in progreso if p["estado"] == "Intento"]
    if intentos:
        return {"estrategia": "refuerzo", "actividad": intentos[0]}

    # ----- Estrategia mejora -----
    completados = [p for p in progreso if p["estado"] == "Completado"]
    if completados:
        return {"estrategia": "mejora", "actividad": completados[0]}

    # ----- Estrategia avance -----
    perfectos = [p for p in progreso if p["estado"] == "Perfecto"]
    if perfectos:
        query_avance = """
        MATCH (a:Alumno {id:$alumno_id})-[:Perfecto]->(act)<-[:TIENE_RAP|:TIENE_CUESTIONARIO|:TIENE_AYUDANTIA]-(unidad:Unidad)
        MATCH (unidad)-[:TIENE_RAP|:TIENE_CUESTIONARIO|:TIENE_AYUDANTIA]->(siguiente)
        WHERE NOT (a)-[:Completado|:Perfecto]->(siguiente)
        RETURN labels(siguiente) AS tipo, siguiente.nombre AS nombre
        LIMIT 1
        """
        record = tx.run(query_avance, alumno_id=alumno_id).single()
        if record:
            return {"estrategia": "avance", "actividad": dict(record)}

    # ----- Si no hay nada que recomendar -----
    return None
